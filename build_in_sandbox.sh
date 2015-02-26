#!/bin/bash

PS4='($LINENO)+ '
set -x
set -e

rm -fr .cabal-sandbox cabal.sandbox.config dist

cabal sandbox init

cabal install Cabal-1.20.0.2
cabal install --haddock-hyperlink-source --dependencies-only --force-reinstalls
cabal install --haddock-hyperlink-source
