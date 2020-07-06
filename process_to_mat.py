#!/usr/bin/env python3
import struct
import math
from pathlib import Path
from progress.bar import Bar
import zstandard as zstd
import numpy 
import os
import glob
from optparse import OptionParser
import time
import hdf5storage
import datetime
import ftplib
#import matlab.engine

def parse_to_list(input):
    tmps=input.split(',')
    out=[]
    #print "input",input
    for tmp in tmps:
      #  print "tmp",tmp
        tmp=tmp.split(':')
       # print "tmp2",tmp,len(tmp)

        
        if len(tmp) == 2:
            for k in range(int(tmp[0]),int(tmp[1])+1):
                out.append(k)
        elif len(tmp) == 1:
            out.append(int(tmp[0]))
        else:
        #    print "bad forma"
            pass
    return out #numpy.asarray(out)


def parse_log(filname):
    beams = []
    for line in open(filname,'r'):
        out = {}
        for word in line.split():
            tmp = word.split('=')
            if len(tmp) == 2:
                key = tmp[0][2:]
                arg = tmp[1]
                if key == 'subbands' or key == 'beamlets' or key =='rcus':
                    arg = parse_to_list(arg)

                if key == 'anadir' or key == 'digdir':
                    tmp = arg.split(',')
                    az = numpy.rad2deg(float(tmp[0]))
                    elv = numpy.rad2deg(float(tmp[1]))
                    ref = tmp[2]
                    arg = {'az':az,'elv':elv,'ref':ref}
                out[key] = arg
                print(key,arg)
            else:
                tmp = word.split('/')
                if len(tmp)>4:
                    out['beam'] = int(tmp[-1][7:])
        #print(out)
        beams.append(out)

       
    return(beams)

def setFileName(rcu_id):
    antenaNo = math.floor(rcu_id/2)
    if rcu_id % 2:
         fileName = str(antenaNo)+'-y'
    else:
        fileName = str(antenaNo)+'-x'
    if antenaNo in beams:
        fileName = beams[antenaNo]+'-'+ fileName
    return fileName+'.bin'    

def get_lane_no(filename):
    name = os.path.basename(filename)
    port_no = int(name[8])-1
    return port_no

def get_beamlets_in_lane(filename):
    cctx = zstd.ZstdDecompressor()

    f = cctx.stream_reader(open(filename,'rb'))
    tbb_data =f.read(7824)
    tbb_format="BBHHbbII3904h"
    tmp = struct.unpack(tbb_format,tbb_data)
    number_of_beamlets=tmp[4]

    f.close()
    return number_of_beamlets


def read_file(filename):
    print('Processing to check if synchronized')
    lane_no = get_lane_no(filename)
    beam_at_lane = get_beamlets_in_lane(filename)
   # bar = Bar('  Processing\n', max=192)
    #bar.start()
    filePath = Path(filename)
    if beam_at_lane == 61:
        beam_format="BBHHbbII3904h"
    if beam_at_lane == 122:
        beam_format="BBHHbbII7808b"
    samples_format= "h"
    clock = 200e6
    missing = 0
    with open(filePath, "rb") as reader:
        print(filePath)
        cctx = zstd.ZstdDecompressor()
        f = cctx.stream_reader(reader)
        #f=reader
        tbb_data =f.read(7824)
        tmp = struct.unpack(beam_format,tbb_data)

        number_of_beamlets=tmp[4]
        number_of_blocks=tmp[5]
        timestamp1=tmp[6]
        timestamp = timestamp1
        print(timestamp)
        block_sequence_number=tmp[7]
        data=tmp[8:]
        data=numpy.asarray(data)
        
        data=numpy.reshape(data,(beam_at_lane,16,4))
        j = (timestamp*clock+512)/1024 + block_sequence_number
        j0 = j
        j1= j
        datax_tmp = []
        datay_tmp =[]
        time_tmp = []
        tmpx=1.0 * data[:,:,0] + 1.j * data[:,:,1]
        tmpy=numpy.squeeze(1.0 * data[:,:,2::4]+1.j * data[:,:,3::4])
        datax_tmp.append(tmpx)
        datay_tmp.append(tmpy)
        time_tmp.append(j)
        
        print(tmpx.shape,tmpy.shape)
        #exit()
        tbb_data = f.read(7824)
        #print('1.',datax[1,:])
        i = 1
        time0 = time.time()
        print('2')
        while tbb_data:
            tmp = struct.unpack(beam_format,tbb_data)
            number_of_beamlets=tmp[4]
            number_of_blocks=tmp[5]
            timestamp=tmp[6]
            block_sequence_number=tmp[7]
            data=tmp[8:]
            data=numpy.asarray(data)
            
            data=numpy.reshape(data,(beam_at_lane,16,4))
            j = (timestamp*clock+512)/1024 + block_sequence_number
            
            while j-j1 > 17:
                missing += 1
                datax_tmp.append(tmpx)
                datay_tmp.append(tmpy)
                time_tmp.append(j)
                j1+=16
                #print('add',j-j1,data.shape)
            j1=j
            i+=1
        
            #if i % 100 == 0:
            #    print('%0.2f ms'%((j-j0)/(200e6/1024)*1000))
            #    print(time.time()-time0)
            #    time0 = time.time()
                #break
            tmpx=1.0 * data[:,:,0] + 1.j * data[:,:,1]
            tmpy=numpy.squeeze(1.0 * data[:,:,2::4]+1.j * data[:,:,3::4])
           
            datax_tmp.append(tmpx)
            datay_tmp.append(tmpy)
            time_tmp.append(j)
          
            tbb_data = f.read(7824)
        print('4')
        #f.close()
    print('3')
    return time_tmp,datax_tmp,datay_tmp,missing



def process_directory(dirname,outdir,configfile,comment='',split=True):
    config = parse_log(configfile)
    print(configfile)
    print(config)
    #exit()
    #outdir = 'out/'
    ext_dir = outdir.split('/')[-1]
    
    ftp = ftplib.FTP('polluks.ise.pw.edu.pl')
    ftp.login('lofar','B!75qZB1')
    # zmiana ktalogu
    ftp.cwd('LOFAR')
    # utowrzenie katalogu
    #ftp.mkd('recent')
    ftp.cwd('recent')
    try:
        ftp.mkd(ext_dir)
    except:
        pass
        
        
    print(ext_dir)
    ftp.cwd(ext_dir)
    print(config)
    for beam in config:
        print(beam)
        beam_no = beam['beam']
        subbands = beam['subbands']
        beamlets = numpy.array(beam['beamlets'])
        print(beam_no,beamlets)
        #    datax_tmp = datax[beamlets,:]
        #   print(datax_tmp.shape)
         
        #  no = beam['
        continue
    #exit()
    #dirname = '/home/julia/data/local/2020-05-13T06:49:55.000/'
    #dirname = '/home/julia/data/local/2020-05-13T07:28:13.000/'
    files = glob.glob(dirname+'/udp_16101*')
    for file1 in files:
        
        file2 = file1.replace("16101","16102")
        file3 = file1.replace("16101","16103").replace('lofar1','lofar2')
        file4 = file1.replace("16101","16104").replace('lofar1','lofar2')
        print(file1,file2,file3,file3)
        
        time_msg_1,x_1,y_1,missing_1 = read_file(file1)
        #print(time_msg_1[0]/(200e6/1024),min(time_msg_1)/(200e6/1024),'ddd')
        #exit()
        time_msg_2,x_2,y_2,missing_2 = read_file(file2)
       
        time_msg_3,x_3,y_3,missing_3 = read_file(file3)
        
        time_msg_4,x_4,y_4,missing_4 = read_file(file4)



        n = len(time_msg_1)
        print('n',n)
        no_subbands,no_times = x_1[0].shape
        datax = numpy.ones([no_subbands*4,n*16],dtype=numpy.complex)
        datay = numpy.ones([no_subbands*4,n*16],dtype=numpy.complex)
        for k in range(len(time_msg_1)):
            datax[0:no_subbands,16*k:16*(k+1)] = x_1[k]
            datay[0:no_subbands,16*k:16*(k+1)] = y_1[k]
            datax[no_subbands*1:no_subbands*2,16*k:16*(k+1)] = x_2[k]
            datay[no_subbands*1:no_subbands*2,16*k:16*(k+1)] = y_2[k]
            datax[no_subbands*2:no_subbands*3,16*k:16*(k+1)] = x_3[k]
            datay[no_subbands*2:no_subbands*3,16*k:16*(k+1)] = y_3[k]
            datax[no_subbands*3:no_subbands*4,16*k:16*(k+1)] = x_4[k]
            datay[no_subbands*3:no_subbands*4,16*k:16*(k+1)] = y_4[k]
           # print(k)
        del x_1,y_1,x_2,y_2,x_3,y_3,x_4,y_4
        for beam in config:
            try:
                beam_no = beam['beam']
                subbands = beam['subbands']
                beamlets = numpy.array(beam['beamlets'])
                print(beamlets)
                datax_tmp = datax[beamlets,:]
                datay_tmp = datay[beamlets,:]
                print(datax_tmp.shape)
                timestamp1 = time_msg_1[0]
                timestamp2 = time_msg_2[0]
                timestamp3 = time_msg_3[0]
                timestamp4 = time_msg_4[0]
                print(numpy.int64(timestamp1/(200e6/1024) ))
                #exit()
                #break
              #  no = beam['
                #continue
                #filename = outdir +'/'+'%3.3d_'%(beam_no) +str(int(timestamp1)) + '_x.mat'
                #filename = outdir +'/' +datetime.datetime.fromtimestamp(int(timestamp1/(200e6/1024)+0.1)).isoformat()+'_%3.3d'%(beam_no) + '_x.mat'
                if split == True:
                    filename = outdir +'/' +datetime.datetime.fromtimestamp(int(timestamp1/(200e6/1024)+0.1)).strftime("%Y%m%d_%H%M%S")+'_%3.3d'%(beam_no) + '_x.mat'
                else:
                    filename = outdir +'/' +datetime.datetime.fromtimestamp(int(timestamp1/(200e6/1024)+0.1)).strftime("%Y%m%d_%H%M%S")+'_%3.3d'%(beam_no) + '.mat'

                print(filename)
                #exit()
                #print(k+lane_no+beam_at_lane,no_subbands)
                print(filename)
                #exit()
                output_data = {}
                output_data['comment']=comment
                if split == True:
                    output_data[u'data'] =  numpy.squeeze(datax_tmp)
                    output_data['polarization']='x'
                else:
                    output_data[u'data_x'] =  numpy.squeeze(datax_tmp)
                    output_data['polarization']='both'
                #output_data[u'y'] = numpy.squeeze(datay_tmp)
                output_data[u'timetick'] = numpy.array([timestamp1,timestamp2,timestamp3,timestamp4])
                output_data[u'missing'] = numpy.array([missing_1*16,missing_2*16,missing_3*16,missing_4*16])
                output_data[u'config'] = beam
                output_data[u'bitmode'] = 32/(2**(no_subbands/61))
                if split == True:
                    hdf5storage.write(output_data,'.',filename,matlab_compatible=True)

                    ftp.storbinary('stor %s'%(os.path.basename(filename)),open(filename,'rb'))

                    os.remove(filename)

                #filename = outdir +'/' +datetime.datetime.fromtimestamp(int(timestamp1/(200e6/1024)+0.1)).isoformat()+'_%3.3d'%(beam_no) + '_y.mat'
                if split == True:
                    filename = outdir +'/' +datetime.datetime.fromtimestamp(int(timestamp1/(200e6/1024)+0.1)).strftime("%Y%m%d_%H%M%S")+'_%3.3d'%(beam_no) + '_x.mat'

                #print(k+lane_no+beam_at_lane,no_subbands)

                #exit()
                output_data = {}
                output_data['comment']=comment
                if split == True:
                    output_data[u'data'] =  numpy.squeeze(datay_tmp)
                    output_data['polarization']='y'
                else:
                    output_data[u'data_y'] =  numpy.squeeze(datay_tmp)

                #output_data[u'y'] = numpy.squeeze(datay_tmp)
                output_data[u'timetick'] = numpy.array([timestamp1,timestamp2,timestamp3,timestamp4])
                output_data[u'missing'] = numpy.array([missing_1*16,missing_2*16,missing_3*16,missing_4*16])
                output_data[u'config'] = beam
                output_data[u'bitmode'] = 32/(2**(no_subbands/61))
                        #print (4<<(no_subbands/61))
                #exit()
                hdf5storage.write(output_data,'.',filename,matlab_compatible=True)
                ftp.storbinary('stor %s'%(os.path.basename(filename)),open(filename,'rb'))
                os.remove(filename)

                #del datax,datay
                #exit()
            except Exception as e:
                print(e)
        del datax,datay
    ftp.quit()
    ftp.close()
