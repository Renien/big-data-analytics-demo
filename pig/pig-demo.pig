-- Load table 'fb_feed'
fb_feed = LOAD '/user/guest/iot-demo/graph-data/sample_fb_data_name_with_header.csv' USING PigStorage(',');
dump fb_feed;
