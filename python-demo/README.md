Execute mapper and reducer:
    cat test.txt | python ./featureFrequency/mapper.py | sort -k1,1 | python featureFrequency/reducer.py | tee logfile.txt

To execute the vis.js network graphs install the following bower_components
    bower install vis and jquery
