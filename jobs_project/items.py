from scrapy import Item, Field

class JobItem(Item):
    _id = Field()  # Auto-generated primary key
    companyName = Field()  # Company name
    correctDate = Field()  # Format: YYYY-MM-DD HH:MM:SS
    jobKey = Field()  # Scraped job key
    jobPageUrl = Field()  # Job posting URL
    annualSalaryAvg = Field()  # Average annual salary (computed)
    city = Field()  # City name only
    zipcode = Field()  # Integer zipcode
