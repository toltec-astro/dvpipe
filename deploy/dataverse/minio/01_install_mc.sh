mc_bin=$HOME/.local/bin/mc
curl https://dl.min.io/client/mc/release/linux-amd64/mc \
  --create-dirs \
  -o ${mc_bin}

chmod +x ${mc_bin}
