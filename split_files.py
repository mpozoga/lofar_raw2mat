#!/usr/bin/env python3
from optparse import OptionParser
import time
import os
import datetime
import subprocess
import glob
import shutil
import process_to_mat
def get_time_range(filename):
    command = './select_range_3 -t %s'%(filename)
    p = subprocess.Popen(command,stdout=subprocess.PIPE,shell=True)
    answer= p.stdout.read().split()
    start = float(answer[0]) #int(int(start/5)*5)
    stop = float(answer[1])
    return start,stop

def round_time(timestamp,interval):
    timestamp = int(timestamp/interval)*interval
    return timestamp
def get_prefix(filename):

    dirname = os.path.dirname(filename)
    name = os.path.basename(filename)
    prefix = name [0:16]
    timefix = name[17:41]
    return prefix,timefix
    
def split_file(filename,outputdir,start,stop,interval=5):
    prefix,timefix = get_prefix(filename)
    print(filename)
    inputdir = os.path.dirname(filename)
    logfile = glob.glob(inputdir+'/*.beamlog')[0]
    
    #external_outputdir = '%s.000'%(datetime.datetime.fromtimestamp(start).isoformat())
    outputdir = '%s/%s.000'%(outputdir,datetime.datetime.fromtimestamp(start).isoformat())
    
    
    for time in range(start,stop,interval):
        command = 'mkdir   %s'%(outputdir)
        
        print(command)
        os.system(command)
        print(logfile)
        shutil.copyfile(logfile,outputdir+'/'+os.path.basename(logfile))
        
        command = './select_range_3 %s --start %s --end %s -o %s/%s.%s'%(filename,datetime.datetime.fromtimestamp(time).isoformat(),datetime.datetime.fromtimestamp(time+interval).isoformat(),outputdir,prefix,datetime.datetime.fromtimestamp(time).isoformat())
        print(command)
        os.system(command)
        name = "%s/%s.%s"%(outputdir,prefix,datetime.datetime.fromtimestamp(time).isoformat())
        #command = " zstd  -1 -f --zstd='strategy=0,wlog=13,hlog=7,slog=1,slen=7'  --rm  %s "%(name)
        command = "zstd -1 --rm -f %s"%(name)
        
        print( command)    
        os.system(command)
        #print(os.path.dirname(name))
        #exit()
    return outputdir
if __name__ == '__main__':
    parser = OptionParser()
    parser.add_option('-d','--directory',dest='dirname',help='path to file searching in data pool directory',default=None)
    parser.add_option('-p','--pool',dest='pool',help='data pool directory where data dir was searched',default='/home/julia/data/lofar?/')
    parser.add_option('-l','--logfile',dest='logfile',help='path to lofar logfile default looking in data directory ',default=None)
    parser.add_option('-t','--time',dest='time',help='time around data was  processed',default=None)
    parser.add_option('-c','--comment',dest='comment',help='reason and comment abaut  observation',default=None)
    parser.add_option('-s','--span',dest='span',help='time range +/- around time, default 5 second',default=5)
    parser.add_option('-o','--output',dest='output',help='output directory where result was stored',default='../data/local/')

    parser.add_option('-r','--remove',action='store_true',help='remove splited file')

    parser.add_option('-i','--split',action='store_true',help='split polaryzation')
    
    (options, args) = parser.parse_args()



    
    if options.remove == True:
        remove = True
    else:
        remove = False
    print(remove)


    if options.split == True:
        split = True
    else:
        split = False


    time_span = int(options.span)

    if options.dirname == None:
        print('No dirname')
        exit()

    pool_name = options.pool

    dirname = options.pool + '/' + options.dirname

    
    if options.logfile == None:
        logfiles = glob.glob(dirname + '/*.beamlog')
        
    else:
        logfiles = glob.glob(options.logfile)
   
    #print(logfiles,len(logfiles))
    if len(logfiles) == 0:
        print('No logfile')
        exit()
    logfile=logfiles[0]

    print(logfile)
    

    outputdir = options.output
        #print(logfile)
    #logfiles = glob.glob(options.logfile)
    
    #print(logfile)
    #print(dirname)
    #exit()

    if options.time != None:
        # process range
        mode = 'range'
        try:
            time_middle = int(options.time)
        except:
            try:
                time_middle =time.mktime( datetime.datetime.fromisoformat(options.time).timetuple())
            except:
                print('wrong time format')
                exit()
        print(time_middle)
        
        #finally:
        #    print('wrog time format')


        print(time_middle)
        
        start_time = time_middle - time_span
        stop_time = time_middle + time_span
    else:
        mode = 'samples'

    
        
    comment = options.comment
        
    
    if mode == 'samples':
        files = glob.glob(dirname+'/*00')
        filename = files[0]
        start, stop = get_time_range(filename)
        start = round_time( (start+stop)/2,5) #+5
            
        for filename in files:
            zstd_dirname = split_file(filename,outputdir,start,start+5)
            print(filename)
            pass

           
    
        #try:
        process_to_mat.process_directory(zstd_dirname,zstd_dirname,logfile,comment,split)
        #except:
        #    pass
        if remove == True:
            command = 'rm -r -f %s'%(zstd_dirname)
            os.system(command)

            
    if mode == 'range':
        files = glob.glob(dirname+'/*00')
        for filename in files:
            start = round_time(start_time,5)
            stop = round_time(stop_time,5)+5
            zstd_dirname = split_file(filename,outputdir,start,stop)            
        print(zstd_dirname)
        
    
        #try:
        process_to_mat.process_directory(zstd_dirname,zstd_dirname,logfile,comment,split)
        #except:
        #    pass
        if remove == True:
            command = 'rm -r -f %s'%(zstd_dirname)
            os.system(command)


    exit()
  

    ##20200603_052141  20200603_065251  20200603_080214  20200603_081636  20200603_082241  20200603_082901  20200603_083600  20200603_084424  20200603_085700


    #works = [{'directory':'/home/julia/data/lofar?/20200603/20200603_085700/','mode':'samples','start':0,'stop':0},]
    works = [{'directory':'/home/julia/data/lofar?/20200603/20200603_065251/','mode':'range','start':1591169017-10,'stop':1591169017+10,'comment':'przelot samolotu w 8 bitach'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_065251/','mode':'range','start':1591169300-10,'stop':1591169300+10,'comment':'przelot samolotu w 8 bitach'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_080214/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 0 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_081636/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 15 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_082241/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 30 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_082901/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 60 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_083600/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 90 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_084424/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 120 stopni'},
             {'directory':'/home/julia/data/lofar?/20200603/20200603_085700/','mode':'samples','start':0,'stop':0,'comment':'obserwacje wszystkimi subaperturami nadajnika elewacja 180 stopni'}  ] 
   
