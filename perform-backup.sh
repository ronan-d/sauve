#!/usr/bin/env bash

set -euo pipefail

storage_device_root=$1

cd

rsync --relative --progress --archive \
  --delete \
  --exclude=target/ \
  --exclude='**llvm-project**build' \
  --exclude=.zig-cache \
  --exclude=_build \
  Documents \
  Music \
  Pictures \
  Projets \
  Videos \
  .bashrc \
  .profile \
  .config/Code/User/settings.json \
  .config/Code/User/keybindings.json \
  .local/share/Anki2 \
  "$1"/Miroir
