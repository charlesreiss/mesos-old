#!/bin/bash
# Create a debian/changelog file based on VCS information. After this changelog
# is created something like 'debuild -us -uc' will create debian packages.
# Arguments: ORIGIN EXTRA_VERSION
#   For git only, ORIGIN is the Apache git SVN checkout upon which the git
# repository is based. Default is 'apache/trunk'.
#   EXTRA_VERSION will be appended to the generated version number. Default is
# the empty string.
#
# The changelog created will give the resulting packages will have a version
# number like '0.0~svn.rSVN_REVISION' if it is a build as in SVN, and
# '0.0~svn.rSVN_REVISION~gitDATE.GIT_COMMIT_ABBREV' if it is a build from a
# GIT repository with commits after the origin SVN. Note that this script does
# not detect and adjust the version number for uncommitted changes.
#
# If this script is called from outside any git or svn repository, the version
# number '0.0~unknown' will be used for the package.

set -x
BASE_VERSION=0.9
VERSION_EXTRA=${2-}

if [ -d .git ]; then
  ORIGIN=${1-apache/trunk}
  GIT_COMMIT=`git rev-list -n 1 HEAD --abbrev-commit`
  ORIGIN_COMMIT=`git rev-list -n 1 $ORIGIN --abbrev-commit`
  SVN_ID=`git log --pretty=format:%B -n 1 $ORIGIN |
          perl -ne 'print $1 if /git-svn-id: [^@]+@(\d+)/'`
  ORIGIN_TIME=$(date -u +%Y%m%d -d "$(git log -n 1 $ORIGIN --pretty=format:%ci)")
  GIT_TIME=$(date -u +%Y%m%d -d "$(git log -n 1 HEAD --pretty=format:%ci)")
  if [ $GIT_COMMIT != $ORIGIN_COMMIT ]; then
    VERSION="$BASE_VERSION~svn.r$SVN_ID~git$GIT_TIME.$GIT_COMMIT"
  else
    VERSION="$BASE_VERSION~svn.r$SVN_ID"
  fi
elif [ -d .svn ]; then
  SVN_ID=`svnversion`
  VERSION="$BASE_VERSION~svn.r$SVN_ID"
else
  VERSION="$BASE_VERSION~unknown"
fi

cat <<EOF >debian/changelog
mesos ($VERSION$VERSION_EXTRA) UNRELEASED; urgency=low

  * Snapshot release.

 -- Apache <general@apache.org>  `date -R`
EOF
