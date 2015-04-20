PIG SCRIPT
==========

###Analysis-1
 
rmf hourly-counts-pig-all
 a = load '/shared/tweets2011.txt' USING PigStorage('\t') as (tweetId: long, userName: chararray, creationTime: chararray, tweetText: chararray);
 b = foreach a generate tweetId, userName, ToDate(creationTime, 'EEE MMM dd HH:mm:ss Z yyyy', 'UTC') as creationTime, tweetText;
 c = Filter b by creationTime > ToDate('2011-01-22') AND creationTime < ToDate('2011-02-09');
 d = foreach c generate tweetId, userName, ToString(creationTime, 'yyyy-MM-dd\'T\'HH') as creationHour, tweetText;
 e = group d by creationHour;
 f = foreach e generate group as creationHour, COUNT(d) as count;
 g = Order f by creationHour;
 h = foreach g generate CONCAT(CONCAT(CONCAT(SUBSTRING(creationHour,5,7), '/'), CONCAT(SUBSTRING(creationHour,8,10),' ')), SUBSTRING(creationHour,11,13)) as creationHour, count;
 store h into 'hourly-counts-pig-all';

###Analysis-2
	
 rmf hourly-counts-pig-egypt
 a = load '/shared/tweets2011.txt' USING PigStorage('\t') as (tweetId: long, userName: chararray, creationTime: chararray, tweetText: chararray);
 b = foreach a generate tweetId, userName, ToDate(creationTime, 'EEE MMM dd HH:mm:ss Z yyyy', 'UTC') as creationTime, tweetText;
 c = Filter b by creationTime > ToDate('2011-01-22') AND creationTime < ToDate('2011-02-09');
 d = Filter c by tweetText MATCHES '.*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*';
 e = foreach d generate tweetId, userName, ToString(creationTime, 'yyyy-MM-dd\'T\'HH') as creationHour, tweetText;
 f = group e by creationHour;
 g = foreach f generate group as creationHour, COUNT(e) as count;
 h = Order g by creationHour;
 i = foreach h generate CONCAT(CONCAT(CONCAT(SUBSTRING(creationHour,5,7), '/'), CONCAT(SUBSTRING(creationHour,8,10),' ')), SUBSTRING(creationHour,11,13)) as creationHour, count;
 store i into 'hourly-counts-pig-egypt';


SPARK SCRIPT
============

###Analysis-1

import org.joda.time.format.DateTimeFormat
 import org.joda.time.DateTimeZone
 
 val raw = sc.textFile("/shared/tweets2011.txt")
 var dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)
 var dateHourFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH").withZone(DateTimeZone.UTC)
 var tweets = raw.map(line => { 
    val format = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withZone(DateTimeZone.UTC)
    var words = line.split("\t")
	if(words.length >= 2){
		var date = format.parseDateTime(line.split("\t")(2))
		((date.getMonthOfYear() + "/" + date.getDayOfMonth() + " " + date.getHourOfDay()),1)
	} else {
		null
	}
	}).filter(x => x != null).reduceByKey(_+_).sortByKey()
	tweets.saveAsTextFile("hourly-counts-spark-all")


###Analysis-2

 import org.joda.time.format.DateTimeFormat
 import org.joda.time.DateTimeZone
 
 val raw = sc.textFile("/shared/tweets2011.txt")
 var dateFormat = DateTimeFormat.forPattern("yyyy-MM-dd").withZone(DateTimeZone.UTC)
 var dateHourFormat = DateTimeFormat.forPattern("yyyy-MM-dd HH").withZone(DateTimeZone.UTC)
 var tweets = raw.map(line => { 
    val format = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z yyyy").withZone(DateTimeZone.UTC)
    var words = line.split("\t")
	if(words.length >= 4){
		if(words(3).matches(".*([Ee][Gg][Yy][Pp][Tt]|[Cc][Aa][Ii][Rr][Oo]).*")) {
			var date = format.parseDateTime(words(2))
			((date.getMonthOfYear() + "/" + date.getDayOfMonth() + " " + date.getHourOfDay()),1)
		} else {
			null
		}
	} else {
		null
	}
	}).filter(x => x != null).reduceByKey(_+_).sortByKey()
	tweets.saveAsTextFile("hourly-counts-spark-egypt")


