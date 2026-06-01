#!/bin/bash

# Setup
echo "Checking if workspace exists..."
WINDOW=(editor console perf)
SESSION="P2P-RustProject"

spawn_workspace() {
  sleep 0.3
  tmux new-session -d -s $SESSION -n "${WINDOW[0]}"
  sleep 0.3
  summon_window "${WINDOW[0]}"
  for w in "${WINDOW[@]:1}"; do
    sleep 0.3
    tmux new-window -t $SESSION -n $w
    summon_window $w
  done
  select_active_window "editor"
}

select_active_window() {
  local session="$SESSION:$1" 
  tmux select-window -t $session 
}

summon_window() {
  local window="$1"
  local target="$SESSION:$window"
  if [ "$window" = "editor" ]; then

    # Dont exist
    if [ -z $FILE ]; then
      tmux send-keys -t $target "nvim" C-m
    else 
      local file="$PWD/${FILE}"
      tmux send-keys -t $target "nvim \"${file}\"" C-m
    fi

  elif [ "$window" = "console" ]; then
    tmux send-keys -t $target C-l
  elif [ "$window" = "perf" ]; then
    tmux send-keys -t $target "btop --force-utf" C-m
  fi
}

main() {
  if tmux has-session -t $SESSION 2>/dev/null; then
    echo "Workspace already exist! restarting..."
    tmux kill-server
    spawn_workspace 
  else 
    # Creating Session
    echo "Creating Workspace..."
    spawn_workspace 
  fi
}

FILE="$1"

main
