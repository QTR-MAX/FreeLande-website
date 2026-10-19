"""FreeLand: read one RCON game-log sample for event-format inspection.

Uses the same mcrcon package as test_rcon.py. Run this from a terminal.
The password is prompted securely and is never written by this script.
An empty response does not prove that logging is disabled.
"""

from datetime import datetime, timezone
from getpass import getpass
from pathlib import Path
import sys


HOST = "31.214.216.36"
PORT = 11040


def main():
    try:
        from mcrcon import MCRcon
    except ImportError:
        print("MISSING_MCRCON: use the Python interpreter that runs test_rcon.py.")
        return 1

    print(f"FreeLand log test - {HOST}:{PORT}")
    print("The admin password will be hidden while you type.")
    password = getpass("Admin password, then press Enter: ")
    if not password:
        print("EMPTY_PASSWORD: no connection attempted.")
        return 1

    stage = "RCON_LOGIN"
    try:
        with MCRcon(HOST, password, port=PORT) as rcon:
            print("RCON_LOGIN_OK")
            stage = "GET_GAME_LOG"
            response = rcon.command("GetGameLog")
    except Exception as error:
        message = str(error).replace(password, "[PASSWORD REDACTED]")
        print(f"{stage}_FAILED: {message}")
        return 1

    # Mask this session's password if it also appears in a returned log entry.
    response = response.replace(password, "[PASSWORD REDACTED]")
    print("\n--- GET_GAME_LOG_RESPONSE ---")
    print(response if response.strip() else "[EMPTY RESPONSE]")
    print("--- END_RESPONSE ---")

    if not response.strip():
        print("No entries returned. Logging may be disabled or no entries are available.")
        return 0

    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f_UTC")
    destination = Path(__file__).resolve().parent / f"rcon_log_sample_{stamp}.txt"
    try:
        destination.write_text(response, encoding="utf-8")
    except OSError as error:
        print(f"SAVE_FAILED: {error}")
        print("The response above can still be copied from this terminal.")
        return 1

    print(f"\nSAMPLE_SAVED: {destination}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nCANCELLED")
        sys.exit(130)
    except EOFError:
        print("\nINPUT_CLOSED: run this file in the VS Code terminal.")
        sys.exit(1)