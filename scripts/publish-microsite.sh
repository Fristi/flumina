#!/bin/bash
set -e

git config --global user.email "mail@markdejong.org"
git config --global user.name "Mark"
git config --global push.default simple

sbt "docs/publishMicrosite"