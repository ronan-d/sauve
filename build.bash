zig build-exe sauve.zig -lc $(pkg-config --libs --cflags libsystemd) -l sqlite3 -O ReleaseSafe
