#!/bin/sh

set -o nounset
set -o errexit

# you can add the following lines to .git/hooks/pre-commit to auto bump build num:
#
# ./version_bump.sh
# git add version.go

VERSION_FILE="../version.go"

usage(){
  echo "Usage: $0 major|minor|patch|build"
  echo "  $0 build (just update buildnum, default)"
  echo "  $0 patch (1.0.PATCH)"
  echo "  $0 minor (1.MINOR.0)"
  echo "  $0 major (MAJOR.0.0)"
}

bump_build(){
  CURRENT_BUILD_NUM=$(grep -o "buildNumber\\s*=\\s*[0-9]\\+" $VERSION_FILE | grep -o "[0-9]\\+")
  NEW_NUM=$((CURRENT_BUILD_NUM+1))
  echo "Bumping build number, from ${CURRENT_BUILD_NUM} to $NEW_NUM"

  sed -i.bak "s/buildNumber[[:space:]]*=[[:space:]]*[0-9]*/buildNumber\\ =\\ ${NEW_NUM}/g" $VERSION_FILE


  NEW_TS=$(date +%s)
  echo "Bumping build timestamp new=$NEW_TS"

  sed -i.bak "s/buildTimeStamp[[:space:]]*=[[:space:]]*[0-9][0-9]*/buildTimeStamp\\ =\\ ${NEW_TS}/g" $VERSION_FILE

  rm -f $VERSION_FILE.bak
  
  # cleanup
  go fmt $VERSION_FILE > /dev/null

  FINAL_VERSION_STRING=$(grep -o "buildVersion\\s*=\\s\"[vV]*[0-9]*\.[0-9]*\.[0-9]*\"\\+" $VERSION_FILE | grep -o "[0-9]*\.[0-9]*\.[0-9]*")
  echo "v${FINAL_VERSION_STRING}"
}

bump_version_string(){
  echo "Bumping ${POSITION} number, from ${XYZ_VERSION_STRING} to ${NEW_VERSION_STRING}"
  sed -i.bak "s/buildVersion[[:space:]]*=[[:space:]]*\"[vV]*[0-9]*\.[0-9]*\.[0-9]*\"/buildVersion\\ =\\ \"${NEW_VERSION_STRING}\"/g" $VERSION_FILE

  bump_build
}


bump_patch(){
  echo "patching"
  NEW_PATCH_NUM=$((PATCH_NUM+1))
  NEW_VERSION_STRING=${MAJOR_NUM}.${MINOR_NUM}.${NEW_PATCH_NUM}
  bump_version_string
}

bump_minor(){
  NEW_MINOR_NUM=$((MINOR_NUM+1))
  NEW_PATCH_NUM="0"
  NEW_VERSION_STRING=${MAJOR_NUM}.${NEW_MINOR_NUM}.${NEW_PATCH_NUM}
  bump_version_string
}

bump_major(){
  NEW_MAJOR_NUM=$((MAJOR_NUM+1))
  NEW_MINOR_NUM="0"
  NEW_PATCH_NUM="0"
  NEW_VERSION_STRING=${NEW_MAJOR_NUM}.${NEW_MINOR_NUM}.${NEW_PATCH_NUM}
  bump_version_string
}




if [ $# -eq 0 ]
then
  bump_build
  exit 0
elif [ $# -gt 1 ]
then
  echo "too many arguments supplied"
  usage
  exit 1
fi


while [ "$1" != "" ]; do

  PARAM=`echo $1 | awk -F= '{print $1}'`
  VALUE=`echo $1 | awk -F= '{print $2}'`

  # should be of form x.y.z
  XYZ_VERSION_STRING=$(grep -o "buildVersion\\s*=\\s\"[vV]*[0-9]*\.[0-9]*\.[0-9]*\"\\+" $VERSION_FILE | grep -o "[0-9]*\.[0-9]*\.[0-9]*")

  MAJOR_NUM=$(echo "$XYZ_VERSION_STRING" | cut -d '.' -f 1)
  MINOR_NUM=$(echo "$XYZ_VERSION_STRING" | cut -d '.' -f 2)
  PATCH_NUM=$(echo "$XYZ_VERSION_STRING" | cut -d '.' -f 3)

  POSITION=$1

  case $PARAM in
      -h | --help)
          usage
          exit 0
          ;;
      build)
        bump_build
        exit 0
        ;;
      patch)
        bump_patch
        exit 0
        ;;
      minor)
        bump_minor
        exit 0
        ;;
      major)
        bump_major
        exit 0
        ;;
      *)
          echo "ERROR: unknown parameter \"$PARAM\""
          usage
          exit 1
          ;;
  esac
  shift
done




