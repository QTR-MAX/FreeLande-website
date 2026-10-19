import getpass
from mcrcon import MCRcon

host = input("Enter RCON IP or hostname: ").strip()

if ":" in host:
    host, port_text = host.rsplit(":", 1)
    port = int(port_text)
else:
    port = int(input("Enter RCON Port: ").strip())

password = getpass.getpass("Enter RCON password: ")

try:
    with MCRcon(host, password, port=port) as rcon:
        print(rcon.command("ListPlayers"))
except Exception as e:
    print("Connection failed:", e)