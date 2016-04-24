#!/bin/sh

if [ ! -e  /tmp/setup ]; then
    mkdir /tmp/setup
fi

pushd /tmp/setup

echo "pwd = `pwd`"
# print current user for better debug
echo "current user is `whoami`"

# install conda
HOME=/home/vagrant
CONDA_HOME=$HOME/conda
CONDA_BIN=$CONDA_HOME/bin/conda

echo "downloading mini conda installer"
wget http://repo.continuum.io/miniconda/Miniconda-3.6.0-Linux-x86_64.sh

echo "miniconda got, start installing"
bash Miniconda-3.6.0-Linux-x86_64.sh -b -f -p $CONDA_HOME

echo "miniconda installed, start install scikit-learn"
$CONDA_BIN install scikit-learn=0.17 matplotlib --yes
echo "scikit-learn, matplotlib installed"

echo "installing nose"
$CONDA_BIN install nose=1.3.7 --yes
echo "nose installed"

echo "installing pandas"
$CONDA_BIN install pandas=0.17.1 --yes
echo "pandas installed"

echo 'PS1="\[\033[0;34m\][\u@\h:\w]$\[\033[0m\]"' >> ~/.bashrc
echo 'export PATH="~/conda/bin:$PATH"' >> ~/.bashrc
echo "export PYTHONHOME=$CONDA_HOME" >> ~/.bashrc

echo "clean up"
# some clean up
if [ -e Miniconda-3.6.0-Linux-x86_64.sh ]; then
    rm -f Miniconda-3.6.0-Linux-x86_64.sh
fi

# message
echo "setup succeed!"
popd
