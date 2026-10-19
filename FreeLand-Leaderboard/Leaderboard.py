"""FreeLand team leaderboard for the ASA log formats verified on this server.

Usage (run with the same Python interpreter as test_logs.py):
    python Leaderboard.py --test     # Preview saved rcon_log_sample_*.txt files.
    python Leaderboard.py            # Collect live events until Ctrl+C.
    python Leaderboard.py --once     # Collect one live response.
    python Leaderboard.py --show     # Show saved live scores without connecting.

Scoring: player kill +1.30, enemy structure destroyed +0.07, player death -1.00.
Points belong to the tribe ID in the event, even if a player later changes tribe.
The preview uses an in-memory database and never imports test scores into live.
Live data and leaderboard.json are stored in the adjacent freeland_data folder.

Only a confirmed human death can validate a 'Your Tribe killed' report. Creature
kill reports alone do not earn points. Global copies of tribe deaths are ignored.
One attacker structure report earns points; the victim's copy earns no penalty.
Unrecognized formats stay in the private raw log database for later inspection.

Live collection uses this server's observed, consumed GetGameLog batches. Use one
collector, and do not run test_logs.py or another GetGameLog poller alongside it.
Keep the collector running: the server's finite buffer cannot provide a complete
history after a long outage. The live RCON path still needs a test on your PC.
"""

from __future__ import annotations

import argparse
from collections import Counter
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from getpass import getpass
from hashlib import sha256
import html
import json
import os
from pathlib import Path
import re
import sqlite3
import sys
import tempfile
import time
from uuid import uuid4


HOST = "31.214.216.36"
PORT = 11040
BASE_DIR = Path(__file__).resolve().parent
POINT_UNITS = {"kill": 130, "structure": 7, "death": -100}
SCHEMA_VERSION = "1"
TIMESTAMP = r"\d{4}\.\d{2}\.\d{2}_\d{2}\.\d{2}\.\d{2}(?:\.\d+)?"
TRIBE_LINE = re.compile(
    rf"^(?P<stamp>{TIMESTAMP}): Tribe (?P<tribe>.+), ID (?P<tid>\d+): "
    r"Day (?P<day>\d+), (?P<clock>\d{2}:\d{2}:\d{2}): (?P<body>.*)$"
)
PLAYER_DEATH = re.compile(
    r"^Tribemember (?P<victim>.+?) - Lvl (?P<level>\d+) "
    r"(?P<cause>(?:was killed|died|starved to death).*)!$"
)
KILL_REPORT = re.compile(
    r"^Your Tribe killed (?P<victim>.+?) - Lvl (?P<level>\d+) "
    r"\((?P<victim_tribe>.+)\)!$"
)
STRUCTURE_REPORT = re.compile(
    r"^(?P<actor>.+?) - Lvl (?P<level>\d+) destroyed their "
    r"'(?P<target>.+)'\)?!$"
)
KILLER_TRIBE = re.compile(r" - Lvl \d+(?:-\d+)? \((?P<tribe>.*)\)$")
TAGS = re.compile(r"<[^>]*>")
EMPTY_REPLIES = {"", "server received, but no response!!", "no response"}


def utc_now():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def clean_message(message):
    result = html.unescape(TAGS.sub("", message)).strip()
    # ASA adds an extra ')' after some rich-text messages in the real samples.
    return re.sub(r"!(?:\s*\))*\s*$", "!", result)


@dataclass(frozen=True)
class TribeRecord:
    stamp: str
    epoch: float
    tribe: str
    tribe_id: str
    day: int
    clock: str
    message: str

    @property
    def fingerprint(self):
        fields = [self.stamp, self.tribe_id, self.day, self.clock, self.message]
        return sha256(json.dumps(fields, ensure_ascii=False).encode("utf-8")).hexdigest()


def parse_tribe_record(line):
    match = TRIBE_LINE.fullmatch(line.strip().lstrip("\ufeff"))
    if not match:
        return None
    stamp = match["stamp"]
    pattern = "%Y.%m.%d_%H.%M.%S.%f" if stamp.count(".") == 5 else "%Y.%m.%d_%H.%M.%S"
    try:
        # Only relative comparisons use this value; display preserves server time.
        epoch = datetime.strptime(stamp, pattern).replace(tzinfo=timezone.utc).timestamp()
    except ValueError:
        return None
    return TribeRecord(
        stamp, epoch, match["tribe"], match["tid"], int(match["day"]),
        match["clock"], clean_message(match["body"]),
    )


class ScoreStore:
    def __init__(self, database=":memory:", server_key=f"{HOST}:{PORT}", preview=False):
        self.preview = preview
        self.db = sqlite3.connect(str(database))
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.executescript("""
            CREATE TABLE IF NOT EXISTS metadata (key TEXT PRIMARY KEY, value TEXT NOT NULL);
            CREATE TABLE IF NOT EXISTS batches (
                batch_id TEXT PRIMARY KEY, received_at TEXT NOT NULL,
                raw_text TEXT NOT NULL, processed INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS tribes (
                tribe_id TEXT PRIMARY KEY, name TEXT NOT NULL, seen_epoch REAL NOT NULL
            );
            CREATE TABLE IF NOT EXISTS records (
                seq INTEGER PRIMARY KEY AUTOINCREMENT, record_id TEXT UNIQUE NOT NULL,
                batch_id TEXT NOT NULL REFERENCES batches(batch_id),
                stamp TEXT NOT NULL, epoch REAL NOT NULL,
                tribe_id TEXT NOT NULL REFERENCES tribes(tribe_id),
                tribe_name TEXT NOT NULL, day INTEGER NOT NULL, clock TEXT NOT NULL,
                message TEXT NOT NULL, kind TEXT,
                victim_name TEXT, victim_level INTEGER, target_tribe TEXT, killer_tribe TEXT
            );
            CREATE TABLE IF NOT EXISTS effects (
                effect_id TEXT PRIMARY KEY, kind TEXT NOT NULL,
                record_id TEXT NOT NULL REFERENCES records(record_id),
                tribe_id TEXT NOT NULL REFERENCES tribes(tribe_id),
                peer_record_id TEXT REFERENCES records(record_id),
                UNIQUE(kind, record_id), UNIQUE(kind, peer_record_id)
            );
            CREATE INDEX IF NOT EXISTS report_matching ON records
                (kind, day, clock, victim_name, victim_level, target_tribe);
        """)
        for key, value in (("schema_version", SCHEMA_VERSION), ("server", server_key)):
            existing = self.db.execute("SELECT value FROM metadata WHERE key=?", (key,)).fetchone()
            if existing and existing[0] != value:
                self.db.close()
                raise ValueError(f"Database {key} differs. Use a separate --data-dir.")
            self.db.execute("INSERT OR IGNORE INTO metadata VALUES (?, ?)", (key, value))
        self.db.commit()

    def close(self):
        self.db.close()

    def capture(self, text, batch_id=None):
        """Commit raw data before parsing; replaying a committed batch is safe."""
        if batch_id is None:
            batch_id = "live-" + uuid4().hex
        with self.db:
            self.db.execute(
                "INSERT OR IGNORE INTO batches(batch_id, received_at, raw_text) VALUES (?, ?, ?)",
                (batch_id, utc_now(), text),
            )
        return batch_id

    def ingest_sample(self, text):
        if not self.preview:
            raise ValueError("Sample ingestion is restricted to the preview database.")
        normalized = "\n".join(line.strip() for line in text.splitlines() if line.strip())
        self.capture(text, "sample-" + sha256(normalized.encode("utf-8")).hexdigest())
        return self.process_pending()

    def _add_record(self, batch_id, index, record, ordinal):
        record_id = (
            f"sample-{record.fingerprint}-{ordinal}" if self.preview
            else f"{batch_id}-{index}"
        )
        self.db.execute("""
            INSERT INTO tribes VALUES (?, ?, ?)
            ON CONFLICT(tribe_id) DO UPDATE SET name=excluded.name, seen_epoch=excluded.seen_epoch
                WHERE excluded.seen_epoch >= tribes.seen_epoch
        """, (record.tribe_id, record.tribe, record.epoch))
        kind, victim, level, target_tribe, killer_tribe = None, None, None, None, None
        death = PLAYER_DEATH.fullmatch(record.message)
        kill = KILL_REPORT.fullmatch(record.message)
        structure = STRUCTURE_REPORT.fullmatch(record.message)
        if death:
            kind, victim, level = "death", death["victim"], int(death["level"])
            killer = KILLER_TRIBE.search(death["cause"])
            killer_tribe = killer["tribe"] if killer else None
        elif kill:
            kind, victim, level = "kill_report", kill["victim"], int(kill["level"])
            target_tribe = kill["victim_tribe"]
        elif structure:
            kind = "structure"
        inserted = self.db.execute("""
            INSERT OR IGNORE INTO records
            (record_id,batch_id,stamp,epoch,tribe_id,tribe_name,day,clock,message,
             kind,victim_name,victim_level,target_tribe,killer_tribe)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            record_id, batch_id, record.stamp, record.epoch, record.tribe_id,
            record.tribe, record.day, record.clock, record.message,
            kind, victim, level, target_tribe, killer_tribe,
        )).rowcount
        if inserted and kind in ("death", "structure"):
            self.db.execute(
                "INSERT OR IGNORE INTO effects VALUES (?,?,?,?,NULL)",
                (f"{kind}-{record_id}", kind, record_id, record.tribe_id),
            )

    def _match_player_kills(self):
        # A matching human death prevents dino kill reports earning player points.
        # Pending reports remain available when the two tribe lines cross a poll.
        reports = self.db.execute("""
            SELECT r.* FROM records r WHERE r.kind='kill_report'
            AND NOT EXISTS (SELECT 1 FROM effects e WHERE e.kind='kill' AND e.record_id=r.record_id)
            ORDER BY r.seq
        """).fetchall()
        for report in reports:
            deaths = self.db.execute("""
                SELECT d.* FROM records d
                WHERE d.kind='death' AND d.day=? AND d.clock=?
                  AND d.victim_name=? AND d.victim_level=? AND d.tribe_name=?
                  AND ABS(d.epoch-?)<=2
                  AND (d.killer_tribe IS NULL OR d.killer_tribe=?)
                  AND NOT EXISTS (
                      SELECT 1 FROM effects e WHERE e.kind='kill' AND e.peer_record_id=d.record_id
                  )
                ORDER BY d.seq
            """, (
                report["day"], report["clock"], report["victim_name"],
                report["victim_level"], report["target_tribe"],
                report["epoch"], report["tribe_name"],
            )).fetchall()
            # Identical names on different tribe IDs are ambiguous: do not guess.
            if not deaths or len({death["tribe_id"] for death in deaths}) != 1:
                continue
            self.db.execute(
                "INSERT OR IGNORE INTO effects VALUES (?,?,?,?,?)",
                ("kill-" + report["record_id"], "kill", report["record_id"],
                 report["tribe_id"], deaths[0]["record_id"]),
            )

    def process_pending(self):
        before = self.effect_count()
        pending = self.db.execute("SELECT * FROM batches WHERE processed=0 ORDER BY rowid").fetchall()
        for batch in pending:
            with self.db:
                occurrences = Counter()
                for index, line in enumerate(batch["raw_text"].splitlines()):
                    record = parse_tribe_record(line)
                    if record is None:
                        continue
                    occurrences[record.fingerprint] += 1
                    self._add_record(batch["batch_id"], index, record, occurrences[record.fingerprint])
                self._match_player_kills()
                self.db.execute("UPDATE batches SET processed=1 WHERE batch_id=?", (batch["batch_id"],))
        return self.effect_count() - before

    def effect_count(self):
        return self.db.execute("SELECT COUNT(*) FROM effects").fetchone()[0]

    def pending_kills(self):
        return self.db.execute("""
            SELECT COUNT(*) FROM records r WHERE r.kind='kill_report' AND NOT EXISTS
            (SELECT 1 FROM effects e WHERE e.kind='kill' AND e.record_id=r.record_id)
        """).fetchone()[0]

    def leaderboard(self):
        teams = []
        for row in self.db.execute("""
            SELECT t.tribe_id,t.name,
                SUM(CASE WHEN e.kind='kill' THEN 1 ELSE 0 END) AS kills,
                SUM(CASE WHEN e.kind='structure' THEN 1 ELSE 0 END) AS structures,
                SUM(CASE WHEN e.kind='death' THEN 1 ELSE 0 END) AS deaths
            FROM tribes t LEFT JOIN effects e ON e.tribe_id=t.tribe_id
            GROUP BY t.tribe_id,t.name
        """):
            units = (row["kills"] * POINT_UNITS["kill"]
                     + row["structures"] * POINT_UNITS["structure"]
                     + row["deaths"] * POINT_UNITS["death"])
            teams.append({
                "tribe_id": row["tribe_id"], "tribe_name": row["name"],
                "kills": row["kills"], "structures_destroyed": row["structures"],
                "deaths": row["deaths"], "points_units": units,
                "points": units / 100, "points_display": f"{units / 100:.2f}",
            })
        teams.sort(key=lambda t: (-t["points_units"], -t["kills"], t["tribe_name"].casefold(), t["tribe_id"]))
        rank, previous = 0, None
        for index, team in enumerate(teams, 1):
            if team["points_units"] != previous:
                rank = index
            team["rank"] = rank
            previous = team["points_units"]
        return teams


def print_board(store, title):
    print(f"\n{title}")
    print("Points = kills x 1.30 + structures x 0.07 - deaths x 1.00")
    print(f"{'Rank':<6}{'Tribe':<30}{'Kills':>8}{'Builds':>9}{'Deaths':>9}{'Points':>12}")
    teams = store.leaderboard()
    if not teams:
        print("No scored tribes yet. Waiting for tribe events.")
    for team in teams:
        print(f"{team['rank']:<6}{team['tribe_name']:<30}{team['kills']:>8}"
              f"{team['structures_destroyed']:>9}{team['deaths']:>9}{team['points_display']:>12}")
    if store.pending_kills():
        print(f"Unconfirmed kill reports: {store.pending_kills()} (may be creature kills; not scored).")


def export_board(store, destination, host, port):
    payload = {
        "schema_version": 1, "generated_at_utc": utc_now(),
        "server": {"host": host, "rcon_port": port},
        "scoring": {"player_kill": 1.3, "structure": 0.07, "player_death": -1},
        "confirmed_effects": store.effect_count(),
        "unconfirmed_kill_reports": store.pending_kills(),
        "teams": store.leaderboard(),
    }
    destination = Path(destination)
    destination.parent.mkdir(parents=True, exist_ok=True)
    temp_name = None
    try:
        with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=destination.parent,
                                         prefix="leaderboard-", suffix=".tmp", delete=False) as output:
            temp_name = output.name
            json.dump(payload, output, ensure_ascii=False, indent=2)
            output.write("\n")
        os.replace(temp_name, destination)
    finally:
        if temp_name and os.path.exists(temp_name):
            os.unlink(temp_name)


@contextmanager
def collector_lock(path):
    """Prevent two copies of this collector reading the same consumed feed."""
    handle = open(path, "a+b")
    acquired = False
    try:
        if os.name == "nt":
            import msvcrt
            if os.fstat(handle.fileno()).st_size == 0:
                handle.write(b"0")
                handle.flush()
            handle.seek(0)
            try:
                msvcrt.locking(handle.fileno(), msvcrt.LK_NBLCK, 1)
            except OSError as error:
                raise RuntimeError("Another FreeLand collector is already running.") from error
        else:
            import fcntl
            try:
                fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except OSError as error:
                raise RuntimeError("Another FreeLand collector is already running.") from error
        acquired = True
        yield
    finally:
        if acquired:
            if os.name == "nt":
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        handle.close()


def preview_samples(paths):
    paths = sorted(set(Path(p).resolve() for p in paths))
    if not paths:
        print("No sample files found. Put Leaderboard.py beside rcon_log_sample_*.txt.")
        return 1
    store = ScoreStore(preview=True)
    try:
        for path in paths:
            text = path.read_text(encoding="utf-8-sig")
            store.ingest_sample(text)
            print(f"READ: {path.name}")
        print_board(store, "FREELAND - SAVED LOG PREVIEW")
        print("\nPREVIEW ONLY: live scores are unchanged; no RCON connection was made.")
        return 0
    finally:
        store.close()


def live_collection(args, store, export_path):
    try:
        from mcrcon import MCRcon
    except ImportError:
        print("MCRcon is missing. Use the Python interpreter that runs test_logs.py.")
        return 1
    password = os.environ.get("FREELAND_RCON_PASSWORD") or getpass("Admin password (hidden): ")
    if not password:
        print("Empty password. No connection attempted.")
        return 1
    print(f"\nLIVE: {args.host}:{args.port} | poll every {args.interval:g}s | Ctrl+C to stop")
    print("Keep this collector running. Use only one GetGameLog reader for this server.")
    last_board = None
    while True:
        try:
            with MCRcon(args.host, password, port=args.port) as rcon:
                print("RCON_LOGIN_OK")
                while True:
                    response = rcon.command("GetGameLog")
                    if response.strip().casefold() not in EMPTY_REPLIES:
                        # Admin/chat lines are not scoring evidence. Mask the secret
                        # there without corrupting timestamps or IDs in tribe events.
                        response = "\n".join(
                            line if TRIBE_LINE.fullmatch(line.strip())
                            else line.replace(password, "[PASSWORD REDACTED]")
                            for line in response.splitlines()
                        )
                        store.capture(response)
                        new_effects = store.process_pending()
                        if new_effects:
                            print(f"NEW_SCORE_EFFECTS: {new_effects}")
                    export_board(store, export_path, args.host, args.port)
                    board = store.leaderboard()
                    if board != last_board:
                        print_board(store, "FREELAND - LIVE TEAM LEADERBOARD")
                        last_board = board
                    if args.once:
                        return 0
                    # GetGameLog can return a capped batch. Drain a full batch
                    # promptly, preserving every same-second structure occurrence.
                    if sum(bool(re.match(rf"^{TIMESTAMP}:", line))
                           for line in response.splitlines()) >= 100:
                        continue
                    time.sleep(args.interval)
        except (OSError, RuntimeError) as error:
            # Database/file failures must stop collection instead of consuming logs.
            if isinstance(error, OSError) and getattr(error, "filename", None):
                raise
            message = str(error).replace(password, "[PASSWORD REDACTED]")
            print(f"RCON_ERROR: {message}")
            if args.once:
                return 1
            print(f"Reconnecting in {args.interval:g}s...")
            time.sleep(args.interval)
        except Exception as error:
            # mcrcon uses its own exception for authentication/protocol failures.
            message = str(error).replace(password, "[PASSWORD REDACTED]")
            if isinstance(error, sqlite3.Error):
                raise
            print(f"RCON_ERROR: {message}")
            return 1


def main(argv=None):
    parser = argparse.ArgumentParser(description="FreeLand ASA team leaderboard")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--test", action="store_true", help="Preview saved test logs without changing live scores")
    mode.add_argument("--show", action="store_true", help="Show saved live scores without RCON")
    mode.add_argument("--once", action="store_true", help="Read one live response and exit")
    parser.add_argument("--samples", nargs="+", type=Path, help="Sample files for --test")
    parser.add_argument("--host", default=HOST)
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--interval", type=float, default=5.0)
    parser.add_argument("--data-dir", type=Path, default=BASE_DIR / "freeland_data")
    args = parser.parse_args(argv)
    if not 1 <= args.port <= 65535 or args.interval < 1:
        parser.error("Use a valid TCP port and an interval of at least 1 second.")
    if args.samples and not args.test:
        parser.error("--samples requires --test")
    if args.test:
        return preview_samples(args.samples or list(BASE_DIR.glob("rcon_log_sample_*.txt")))
    args.data_dir.mkdir(parents=True, exist_ok=True)
    with collector_lock(args.data_dir / "collector.lock"):
        store = ScoreStore(args.data_dir / "freeland.sqlite3", f"{args.host}:{args.port}")
        try:
            store.process_pending()
            output = args.data_dir / "leaderboard.json"
            export_board(store, output, args.host, args.port)
            if args.show:
                print_board(store, "FREELAND - SAVED LIVE SCORES")
                return 0
            print(f"DATA: {args.data_dir.resolve()}")
            print(f"JSON: {output.resolve()}")
            return live_collection(args, store, output)
        finally:
            store.close()


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nSTOPPED: saved live scores are retained.")
        sys.exit(0)
    except (OSError, sqlite3.Error, RuntimeError, ValueError, EOFError) as error:
        print(f"ERROR: {error}")
        sys.exit(1)
