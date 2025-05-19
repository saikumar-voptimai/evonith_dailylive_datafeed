I am scripting this python based code that runs using CLI and hence the argument parser.

Like in my current role, we need a service that runs every night to load the data to any
timeseries/other db and here we have chosen InfluxDB.

The script is triggered at around 12.30AM in the morning, every day and it fetches all the data for previous day. For example, on 03rd of April, it fetches the data for 02nd of April and flush writes it to the InfluxDB bucket_raw Bucket.

Parallely, it does the Downsampling, for example, takes 5min average of the data and writes it to
bucket_5min.

Other than that, we can use the argparser where we write data on specific dates using the date-range.
If the values on these dates are unavailable or if they are significantly different, the values are 
written.

Else left over.

