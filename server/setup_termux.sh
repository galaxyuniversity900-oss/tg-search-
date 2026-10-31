#!/data/data/com.termux/files/usr/bin/sh
# TG-Atlas installer for Termux.
# Usage: put tg_atlas.py and this file in the same folder, then run:  sh setup_termux.sh
set -e
DIR="$HOME/tg-atlas"

echo "[1/4] Installing Python..."
pkg update -y
pkg install -y python

echo "[2/4] Installing Telethon..."
pip install --upgrade telethon

echo "[3/4] Copying files to $DIR ..."
mkdir -p "$DIR" "$HOME/bin"
SRC="$(cd "$(dirname "$0")" && pwd)"
cp "$SRC/tg_atlas.py" "$DIR/tg_atlas.py"
[ ! -f "$SRC/tg_atlas_config.json" ] || cp "$SRC/tg_atlas_config.json" "$DIR/tg_atlas_config.json"
chmod 600 "$DIR/tg_atlas_config.json" 2>/dev/null || true

echo "[4/4] Creating the 'atlas' command..."
cat > "$HOME/bin/atlas" <<'LAUNCH'
#!/data/data/com.termux/files/usr/bin/sh
# atlas        -> start the server in the background and open the UI
# atlas stop   -> stop the server
# atlas log    -> show the server log
cd "$HOME/tg-atlas" || exit 1
case "$1" in
  stop) pkill -f "tg_atlas.py serve" && echo "Stopped." || echo "Not running."; exit 0 ;;
  log)  tail -n 40 atlas.log; exit 0 ;;
esac
termux-wake-lock 2>/dev/null   # keep running when the screen is off
if ! pgrep -f "tg_atlas.py serve" >/dev/null; then
  nohup python tg_atlas.py serve > atlas.log 2>&1 &
  sleep 3
fi
URL=$(grep -o 'http://127.0.0.1:[0-9]*/?t=[A-Za-z0-9_-]*' atlas.log | tail -1)
if [ -z "$URL" ]; then echo "Server did not start. Run: atlas log"; exit 1; fi
echo "Open: $URL"
termux-open "$URL" 2>/dev/null || true
LAUNCH
chmod +x "$HOME/bin/atlas"

# make ~/bin available in every new session
grep -q 'HOME/bin' "$HOME/.profile" 2>/dev/null || echo 'export PATH="$HOME/bin:$PATH"' >> "$HOME/.profile"
export PATH="$HOME/bin:$PATH"

echo
echo "Done. Start TG-Atlas any time with:  atlas"
