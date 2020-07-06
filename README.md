Program do konwersji plików z beamformingu zalogowanych z udp do formatu .mat

Usage: split_files.py [options]

Options:
  -h, --help            show this help message and exit
  -d DIRNAME, --directory=DIRNAME
                        path to file searching in data pool directory
  -p POOL, --pool=POOL  data pool directory where data dir was searched
  -l LOGFILE, --logfile=LOGFILE
                        path to lofar logfile default looking in data
                        directory
  -t TIME, --time=TIME  time around data was  processed
  -c COMMENT, --comment=COMMENT
                        reason and comment abaut  observation
  -s SPAN, --span=SPAN  time range +/- around time, default 5 second
  -o OUTPUT, --output=OUTPUThf ./
                        output directory where result was stored
  -r, --remove          remove splited file
  -i, --split           split polaryzation



Working example:

./split_files.py  -d 20200603/20200603_065251 -t 2020-01-01T12:00:00 -t 1591169300 -r -c 'testowy przebieg programu' --split
