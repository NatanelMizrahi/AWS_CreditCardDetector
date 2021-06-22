# AWS Documents Credit Card Detector
A python app that asynchronously parses documents in a given AWS S3 bucket and creates a CSV with credit card info to detect vulnerabilities.   
## How to Use
0. Run `pip install -r requirements.txt`
1. Edit AWS Bucket name, Access ID and Access key in  `config/credentials.py`
2. Run `python3 app.py`
3. View output in `output/out.csv`
## Notes
* High level UML sequence diagram in `docs/SequenceDiagram.png`

* Credit card patterns are scraped and cached from the HTML tables [here](https://en.wikipedia.org/wiki/Payment_card_number) and [here](http://baymard.com/checkout-usability/credit-card-patterns). This is also configurable and additional sources can be added.
* Each match in `output/out.csv` has a *valid* column value, which denotes whether the prefix is a valid credit card IIN number and the length is in the range of the respective issuing network.
* [Luhn's Algoithm](https://en.wikipedia.org/wiki/Luhn_algorithm) validation is also used and presented in the csv. 
* Misc. app config in `config/app_config.py`
* Several unit tests in `tests`