## Read the sample data 

# Read the directory 
setwd('/Users/renienj/Desktop/iot-meetup-bigdata-analytics/demo/r-language')
getwd()
list.files()

# Read the tsv file
fb_data <- read.csv('sample_fb_data.tsv', header = TRUE, sep = '\t')

# Get all the headers
names(fb_data)

## Analyse user birthday

# install.packages('ggplot2')
library(ggplot2)

# DOB Days Histogram
qplot(x = dob_day, data = fb_data) 

# Frequency plot for friends count 
qplot(data = fb_data, x = friend_count, binwidth = 25, color = I('black'), fill = I('#099DD9')) +
  theme(axis.text.x=element_text(color = "black", size=11, angle=30, vjust=.8, hjust=0.8)) +
  scale_x_continuous(limits = c(0, 1000), breaks = seq(0, 1000, 50))

# Data as function of gender
qplot(data = subset(fb_data, !is.na(gender)), x = friend_count, binwidth = 25,
      color = I('black'), fill = I('#099DD9')) +
  theme(axis.text.x=element_text(color = "black", size=11, angle=30, vjust=.8, hjust=0.8)) +
  scale_x_continuous(limits = c(0, 1000), breaks = seq(0, 1000, 100)) +
  facet_wrap(~gender)

# Age of the FB users
qplot(data = fb_data, x = age, binwidth = 1,
      color = I('black'), fill = I('#5760AB')) +
  scale_x_continuous(breaks = seq(8,113,5), lim = c(8,113)) +
  xlab('Age of Users in Years') +
  ylab('Number of users in sample')