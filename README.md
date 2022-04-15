# Setup fo YouTubeTrans Development

The YouTubeTrans project has three backend modules written in Python.  Each runs in a dedicated virtual environment.

- Step 1 : (ytplaylister.py) download a playlist for GetTrans.py to consume.
- Step 2 : (GetTrans.py) download transcripts for youtube into a project corpus folder.
- Step 3 : (SearchTrans.py) search the transcripts and output video bookmarks

Requires python above version 3.5.

## 4. Install required python packages in virtual env (linvenv)
``` 
python -m venv linvenv
. ./linenv/bin/activate
# verify in virtual env with path of the followin command
pip -V
```
pip install pandas
pip install youtube_transcript_api
pip install wordcloud
pip install cursor
pip install tdqm

```

## Test the Getting and Searching of the YouTube Transcripts

  There is a test job file for getting started and testing everything is working.

### **GetTrans** Functionallity Test

