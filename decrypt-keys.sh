#!/bin/sh

openssl aes-256-cbc -K $encrypted_0bef3c8cbb27_key -iv $encrypted_0bef3c8cbb27_iv -in travis-deploy-key.enc -out travis-deploy-key -d;
chmod 600 travis-deploy-key;
cp travis-deploy-key ~/.ssh/id_rsa;