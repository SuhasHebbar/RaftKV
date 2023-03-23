#!/bin/bash
echo Starting setup script

if [ -f "$HOME/.ssh/known_hosts" ]; then
  if [ ! -n "$(grep "^github.com" $HOME/.ssh/known_hosts)" ]; then
    ssh-keyscan -t rsa github.com >> $HOME/.ssh/known_hosts 2>/dev/null;
  fi
fi

chmod 400 ~/.ssh/id_ed25519*

if [[ "" == "$(ls setup_19.x)" ]]; then
	curl -fsSL https://deb.nodesource.com/setup_19.x -O 
	sudo -E bash setup_19.x
fi

if [[ "" == "$(ls -ald "$HOME"/.asdf)" ]]; then
  git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.11.3
  echo ". ~/.asdf/asdf.sh" >> ~/.bashrc
  echo ". ~/.asdf/completions/asdf.bash" >> ~/.bashrc
  . ~/.asdf/asdf.sh
  . ~/.asdf/completions/asdf.bash
fi



if [[ "" == "$(which go)" ]]; then
  asdf plugin add golang
  asdf install golang 1.20.2
  asdf global go 1.20.2
fi

sudo apt update >> /dev/null
sudo apt install -y htop python-is-python3 python3-plumbum nodejs hyperfine ripgrep trash-cli asciinema libfuse2

if [[ ":$PATH:" != *":$HOME/bin:"* ]]; then
	echo Adding \$HOME/bin to path
	mkdir -p ~/bin
	echo "export PATH=\$HOME/bin:\$PATH" >> ~/.bashrc
fi

if [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
	echo Adding \$HOME/.local/bin to path
	mkdir -p ~/bin
	echo "export PATH=\$HOME/.local/bin:\$PATH" >> ~/.bashrc
fi

proj_name=kv

mkdir -p "$HOME"/suhas

if [[ "" == "$(ls -ald suhas/$proj_name)" ]]; then
  git clone git@github.com:SuhasHebbar/CS739-P2.git suhas/$proj_name 
  pushd suhas/$proj_name || exit
  git config user.name "Suhas Hebbar"
  git config user.email "hebbar2@wisc.edu"
  popd || exit

fi

if [[ ":$PATH:" != *":$HOME/go/bin:"* ]]; then
	echo Adding \$HOME/go/	echo Adding \$HOME/bin to pathbin to path
	mkdir -p ~/go/bin
	echo "export PATH=\$HOME/go/bin:\$PATH" >> ~/.bashrc
fi

if [[ "" == "$(which protoc-gen-go)" || "" == "$(which protoc-gen-go-grpc)" ]]; then
	go install google.golang.org/protobuf/cmd/protoc-gen-go@v1.28
	go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@v1.2
fi


if [[ "" == "$(which nvim)" ]]; then
	curl -L https://github.com/neovim/neovim/releases/latest/download/nvim.appimage -o ~/bin/nvim
	chmod u+x ~/bin/nvim
fi

rm -rf ~/.config/nvim
rm -rf ~/hebbar2_config
mkdir -p ~/.config

git clone https://github.com/SuhasHebbar/config hebbar2_config
ln -sf ~/hebbar2_config/nvim ~/.config/nvim

git config --global user.name "Group 1"
git config --global user.email "group1@not_a_real_email.com"

# sudo apt remove --purge -y neovim clangd cmake
# rm -rvf ~/grpc
echo Finished setup script
