#!/usr/bin/bash
# -----------------------------------------------------------------------------
# Create or update Corpus Transcript / wordclouds driver script
# -----------------------------------------------------------------------------
AppDir=${APPDIR_YTTRANS}
VenvCmd_wsl="${AppDir}/linvenv/bin/activate"
SrcTransDir="../transcripts"
VenvCmd_wsl="./linvenv/bin/activate"

Enable_upload_trans_data=1

while getopts c:p:l:s:e: flag; do
    case "${flag}" in
    c) corpus=${OPTARG} ;;
    p) playlist=${OPTARG} ;;
    l) lang=${OPTARG} ;;
    s) stopwords=${OPTARG} ;;
    e) enable_cmd=${OPTARG} ;;
    esac
done

if [[ ! $corpus || ! $playlist ]]; then
    exit 42
fi

file_playlist="../generated_playlists/playlist_v3_${corpus}_${playlist}.csv"

Process_getTrans() {
    echo "Process - Get Transcripts"
    echo "Corpus is ${corpus}"
    echo "lang is ${lang}"

    cmd_str="cd ${AppDir} && source ${VenvCmd_wsl} && python GetTrans.py"
    cmd_str+=" -p -w"
    cmd_str+=" --corpus ${corpus}"
    cmd_str+=" --lang ${lang}"

    cmd_str+=" --youText ${AppDir}/${file_playlist}"
    cmd_str+=" --db ${corpus}_Transcripts.db"
    cmd_str+=" -l debug"
    echo "$cmd_str"
    eval "${cmd_str}"
}
#  cmd_str+=" --stopwords ${stopwords[*]}"
Process_getTrans
