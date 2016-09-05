=========
Functionality
=========

Calculates amount as well as sum of bidding price for each IP in incoming dataset.

=========
Building
=========
mvn package -Dmaven.test.skip=true


=========
Usage
=========
yarn jar bidding-counter <input_dir> <output_dir>

Input directory is expected to contain dataset(s) for processing
Format:
Bid ID\tTimestamp\tiPinyou ID\tUser-Agent\tIP(*)\tRegion\tCity\t\tAd Exchange\tDomain\tURL\tAnonymous URL ID\tAd slot ID\tAd slot width\tAd slot height\tAd slot visibility\tAd slot format\tPaying price\tCreative ID\tBidding price\tAdvertiser ID\tUser Tags\tStream ID

Output directory must not exist.

