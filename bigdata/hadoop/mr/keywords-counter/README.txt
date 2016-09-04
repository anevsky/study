=========
Functionality
=========

Calculates amount of each keyword (tag) in incoming dataset.

=========
Building
=========
mvn package -Dmaven.test.skip=true


=========
Usage
=========
yarn jar keywords-counter <input_dir> <output_dir>

Input directory is expected to contain dataset(s) for processing
Format:
ID\tKeyword Value\tKeyword Status\tPricing Type\tKeyword Match Type\tDestination URL

Output directory must not exist.

