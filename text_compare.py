#!/usr/bin/env python
"""
Compare 2 files
Generate a report with following stats:

Generate differences

The script takes a config file with following parameters as an argument
fileA - (Mandatory) First file to be taken for comparision
fileB - (Mandatory) Second file to be taken for comparision
reportfile - (Mandatory) Name of the output report file
fileADel - (Needed for delimited files) The delimiter for fileA
fileBDel - (Needed for delimited files) The delimiter for fileB
metafile=
keyfields=
ignorefields
skipRecs - (Mandatory for delimited files)
fileAOnly
fileBOnly
keyMismatchThreshold - (optional) Number of mismatch samples to report
"""

import sys
import csv
from datetime import datetime as dt
from collections import defaultdict, OrderedDict

def show_progress(iteration, total, prefix='', suffix='', decimals=1, barlength=50, clrlen=100):
    """
    Call in a loop to create terminal progress bar
    @params:
        iteration   - Required  : Current iteration (Int)
        total       - Required  : total iterations (Int)
        prefix      - Optional  : prefix string (Str)
        suffix      - Optional  : Suffix string (Str)
        decimals    - Optional  : Positive number of decimals in percent complete (Int)
        barlength   - Optional  : Character length of the bar (Int)
    """
    clrstr = ' ' * clrlen
    sys.stdout.write('\r%s' % clrstr)
    sys.stdout.flush()
    format_str = "{0:." + str(decimals) + "f}"
    percents = format_str.format(100 * (iteration/float(total)))
    filled_len = int(round(barlength * iteration/float(total)))
    bar = chr(130) * filled_len + '-' * (barlength - filled_len)
    sys.stdout.write('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix))
    if iteration == total:
        sys.stdout.write('\n')
    sys.stdout.flush()
    return len('\r%s |%s| %s%s %s' % (prefix, bar, percents, '%', suffix))

def delimit(line, lengths):
    """Accept a string as 'line' and yeilds fields of 'lengths' passed """ 
    offset = 0
    for length in lengths:
        yield line[offset:offset+length]
        offset += length

def timestamp(message):
    """Write a message to stdout with the current timestamp"""
    sys.stdout.write(dt.now().strftime("%x %X") + (': %s \n'% message))

def parse_config(configfile):
    """Parse the config file 'configfile' and return the parsed key-value pairs as dict"""
    return {k:v for k,v in map(lambda x: x.strip().split('='), filter(lambda x: not x.strip().startswith('#'), 
        (line for line in open(configfile))))}

def get_key(data, keyfields):
    """Return a tuple of key with the data and keyfields indexes passed"""
    return tuple([data[i-1] for i in map(int, keyfields.split(','))])

def write_exclusive_recs(config, filename, file_dict, excl_keys):
    '''
    write a file with the records exclusive to a given side of the compare
    '''
    if excl_keys:
        with open(filename,'w') as xonly_file:
            xonly_file.writelines( [''.join(file_dict[_]) + '\n' for _ in excl_keys ])

def get_diff(seqA, seqB, keyfields=None, ignorefields=None):
    """Takes following as parameters:
        sequenceA, sequenceB, 
        list of indices that make the key for the record, 
        list of indices for the the fields that may be ignored
    Returns a list of tuples in the form
    (index of the field in the record, value on sideA, value on sideB, key for the record)"""
    if keyfields:
        if get_key(seqA, keyfields) != get_key(seqB, keyfields):
            raise KeyError ('Key mismatched')
    elif len(seqA) != len(seqB):
        raise ValueError ('LengthMismatch')
    return [(i,a,b, get_key(seqA, keyfields)) for i, (a,b) in enumerate(zip(seqA, seqB)) if a != b and str(i+1)] # Bug!! not in ignorefields]

def main(configfile):
    '''
    Does the following:
    1. Parse the configfile to setup the compare parameters
    2. Based on whether the files are delimited or fixed length, read the files as dictionaries with key as the record key and value as the record
    3. This gives us 2 dictionaries dictA, dictB
    4. Find the keys common between 2 dicts
    5. Find the records that are exclusive to one file and write those to report files if needed
    '''
    timestamp('Process Start')
    config = parse_config(configfile)
    diff_count = defaultdict(int) # To store counts of various mismatched fields

    try:
        file_fields = [ line.strip() for line in open(config['metafile']).readlines() ]
    except KeyError:
        file_fields = None

    rec_matched = 0
    diff_samples = []

    if 'fileADel' in config and 'fileBDel' in config:
        fileA = csv.reader(open(config['fileA'], 'rb'), delimiter=config['fileADel'])
        fileB = csv.reader(open(config['fileB'], 'rb'), delimiter=config['fileBDel'])
        t = show_progress(1,6, prefix='Initial Setup', suffix='Scanning FileA')
        dictA = OrderedDict( (get_key(x, config['keyfields']), x) for _, x in enumerate(fileA) if _ >= int(config['skipRecs']) )
        t = show_progress(2,6, prefix='Initial Setup', suffix='Scanning FileB', clrlen=t)
        dictB = OrderedDict( (get_key(x, config['keyfields']), x) for _, x in enumerate(fileB) if _ >= int(config['skipRecs']) )
        t = show_progress(3,6, prefix='Initial Setup', suffix='Finding Common keys', clrlen=t)
    elif 'colwidths' in config:
        with open(config['fileA'], 'rb') as fileA, open(config['fileB'], 'rb') as fileB:
            lengths = [ int(x) for x in config['colwidths'].split(',') ]
            t = show_progress(1,6, prefix='Initial Setup', suffix='Scanning FileA')
            dictA = OrderedDict( (get_key(tuple(delimit(x,lengths)), config['keyfields']), tuple(delimit(x,lengths))) for _, x in enumerate(fileA) if _ >= int(config['skipRecs']) )
            t = show_progress(2,6, prefix='Initial Setup', suffix='Scanning FileB', clrlen=t)
            dictB = OrderedDict( (get_key(tuple(delimit(x,lengths)), config['keyfields']), tuple(delimit(x,lengths))) for _, x in enumerate(fileB) if _ >= int(config['skipRecs']) )
            t = show_progress(3,6, prefix='Initial Setup', suffix='Finding Common keys', clrlen=t)
    else:
        print ('Missing Delimiter or column with information in config file. Aborting!')
        sys.exit(1)

    common_keys = set(dictA.keys()).intersection(set(dictB.keys()))
    t = show_progress(4,6, prefix='Initial Setup', suffix='Finding Aonly recs', clrlen=t)
        
    aonly_keys = set(dictA.keys()) - set(dictB.keys())
    if 'fileAOnly' in config:
        write_exclusive_recs(config, config['fileAOnly'], dictA, aonly_keys)

    t = show_progress(5,6, prefix='Initial Setup', suffix='Finding Bonly recs', clrlen=t)
    bonly_keys = set(dictB.keys()) - set(dictA.keys())
    if 'fileBOnly' in config:
        write_exclusive_recs(config, config['fileBOnly'], dictB, bonly_keys)

    show_progress(6,6, prefix='Initial Setup', suffix='Initial setup complete', clrlen=t)
    timestamp("End Initial Setup & File Read")

    l = len(common_keys)
    pct = l/100.0
    for (i,k) in enumerate(common_keys,start=1):
        if i % pct == 0:
            show_progress(i,l, prefix='Comparision Progress', suffix=str(i) )           
        try:
            if 'ignorefields' in config:
                diffs = get_diff(dictA[k], dictB[k], config['keyfields'], config['ignorefields'])
            else:
                diffs = get_diff(dictA[k], dictB[k], config['keyfields'])
        except ValueError:
            print ("LengthMismatch in line number", i)
            continue
        except KeyError:
            print ("keyMismatch in line number", i)
        else:
            num_fields = len(dictA)
            if diffs:
                for diff in diffs:
                    diff_count[diff[0]] += 1
                    if 'keyMismatchThreshold' in config:
                        if diff_count[diff[0]] <= int(config['keyMismatchThreshold']):
                            diff_samples.append((i,diff))
                    else:
                        diff_samples.append((i,diff))
            else:
                rec_matched += 1
    
    show_progress(i,l, prefix='Comparision Progress', suffix="Done")    
    timestamp('End Comparision')

    if not file_fields:
        file_fields = ['Column ' + str(x + 1) for x in xrange(num_fields)]
    rows_cmprd = i - ( int(config['skipRecs']) or 0 )

    with open(config['reportfile'],'wb') as rptfile:
        rptwriter = csv.writer(rptfile, dialect='excel')
        rptwriter.writerow(['fileA', config['fileA'], len(dictA)])
        rptwriter.writerow(['fileB', config['fileB'], len(dictB)])
        rptwriter.writerow(['Number of recs exclusive to FileA', len(aonly_keys)])
        rptwriter.writerow(['Number of recs exclusive to FileB', len(bonly_keys)])
        if 'fileAOnly' in config:
            if aonly_keys:
                rptwriter.writerow( ['FileA Only recs written to: ',  config['fileAOnly']])
        if 'fileBOnly' in config:
            if bonly_keys:
                rptwriter.writerow( ['FileB Only recs written to: ',  config['fileBOnly']])
        rptwriter.writerow(["Rows compared", len(common_keys)])
        rptwriter.writerow(["Rows matched", rec_matched])
        rptwriter.writerow(["Rows mismatched", len(common_keys) - rec_matched])
        rptwriter.writerow([])
        rptwriter.writerow(["Data Element mismatched stats:"])
        rptwriter.writerow(["Field Name", 'Diff Count'])

        for key in diff_count:
            rptwriter.writerow( [ file_fields[key], diff_count[key]])
        rptwriter.writerow([])
        rptwriter.writerow(["Sample differences:"])
        rptwriter.writerow(["Line#"] + list(get_key(file_fields, config['keyfields'])) + ["Field Name", "FileA Value", "FileB Value"])
        l = len(diff_samples)
        pct = l/100.0
        for i,d in enumerate(diff_samples, start=1):
            if i % pct == 0:
                show_progress(i,l, prefix='Creating Report', suffix=str(i) )
            row = [d[0]] +  list(d[1][3]) + [file_fields[d[1][0]], d[1][1], d[1][2]]
            rptwriter.writerow(row)
        show_progress(i,l, prefix='Creating Report', suffix="Done")
    timestamp('End Report Generation')
    print ("\nComplete!")

if __name__ == "__main__":
    """ argv[1] is the full name of the config file"""
    main(sys.argv[1])
