import os
from datetime import datetime
import re
import json
import scrapy
from ..items import JobItem
import logging
import uuid

class JsonSpider(scrapy.Spider):
    name = 'json_spider'
    
    def start_requests(self):
        base_dir = '/app/data'
        files = ['s01.json', 's02.json']
        
        for file in files:
            file_path = os.path.join(base_dir, file)
            yield scrapy.Request(
                url=f'file://{file_path}',
                callback=self.parse,
                cb_kwargs={'filename': file}
            )

    def parse(self, response, filename):
        data = json.loads(response.text)
        jobs = data.get('jobs', [])
        
        for job_raw in jobs:
            try:
                # Get the data field which contains most job information
                job_data = job_raw.get('data', {})
                if not job_data:
                    continue
                    
                item = JobItem()
                item['_id'] = str(uuid.uuid4())
                
                # Extract data using dedicated functions
                item['correctDate'] = self.extract_date(job_raw)
                item['companyName'] = self.extract_company(job_data)
                item['annualSalaryAvg'] = self.extract_salary(job_data)
                item['jobKey'] = self.extract_job_key(job_data)
                item['jobPageUrl'] = self.extract_job_url(job_data)
                item['city'] = self.extract_city(job_data)
                item['zipcode'] = self.extract_zipcode(job_data)
                
                yield item
                
            except Exception as e:
                self.logger.error(f"Error parsing job data: {str(e)}")
                continue

    def extract_id(self):
        """Generate unique ID for job listing"""
        return str(uuid.uuid4())

    def extract_city(self, job_data):
        """Extract and normalize city name from job data"""
        # Try full_location first
        location = job_data.get('full_location', '')
        if location:
            parts = location.split(',')
            if len(parts) >= 2:
                city = parts[0].strip()
        else:
            # Try direct city field as fallback
            city = job_data.get('city', '')
        
        if not city:
            return None
        
        # Normalize the city name
        city = city.strip()
        
        # Convert to title case while preserving special cases
        words = city.lower().split()
        normalized_words = []
        
        for i, word in enumerate(words):
            # Special case for French "en" and "la"
            if word in ["en", "la"] and i != 0:
                normalized_words.append(word.lower())
                continue
            
            # Special case for directionals in multi-word cities
            if word in ["north", "south", "east", "west"] and i == 0:
                normalized_words.append(word.title())
                continue
            
            # Special case for Saint/St.
            if word in ["saint", "st", "st."]:
                normalized_words.append("St.")
                continue
            
            # Special case for Mc surnames
            if word.startswith("mc"):
                normalized_words.append("Mc" + word[2:].title())
                continue
            
            # Special case for CDG (airport code)
            if word.lower() == "cdg":
                normalized_words.append("CDG")
                continue
            
            # Default case
            normalized_words.append(word.title())
        
        return " ".join(normalized_words)

    def extract_date(self, job_raw):
        """Extract and format the job posting date"""
        job_data = job_raw.get('data', {})
        date_str = job_data.get('create_date')
        if date_str:
            try:
                # Parse ISO format date string
                date_obj = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S%z")
                # Format to match database format: YYYY-MM-DD HH:MM:SS
                return date_obj.strftime("%Y-%m-%d %H:%M:%S")
            except (ValueError, TypeError) as e:
                self.logger.error(f"Error parsing date: {e}")
        return None

    def extract_company(self, job_data):
        # Try hiring_organization first
        company = job_data.get('hiring_organization')
        if company:
            return company.strip()
        
        # Try brand as fallback
        brand = job_data.get('brand')
        if brand:
            return brand.strip()
        
        return "Unknown Company"

    def extract_salary(self, job_data):
        """Extract and normalize salary information from job data"""
        # First try to get salary from salary fields
        avg_salary = job_data.get('salary_value', 0)
        if avg_salary:
            return avg_salary * 40 * 52  # Convert to annual salary

        min_salary = job_data.get('salary_min_value', 0)
        max_salary = job_data.get('salary_max_value', 0)
        
        if min_salary and max_salary:
            avg_salary = (min_salary + max_salary) / 2
            return avg_salary * 40 * 52  # Convert to annual salary
        
        # If no salary range, try to extract from description
        description = job_data.get('description', '')
        if not description:
            return None
        
        description = ' '.join(description.lower().split())

        # Common patterns for hourly wages
        min_max_patterns = [
            r'\$(\d+\.?\d*)\s*per hour to \$(\d+\.?\d*)', # $18.75 per hour to $19.25
            r'\$(\d+\.?\d*)\s*-\s*\$(\d+\.?\d*)', # $18.75 - $19.25
            r'\$(\d+\.?\d*)\s*to \$(\d+\.?\d*)', # $18.75 to $19.25
            r'\$(\d+\.\d+)\s*(?:-|to)\s*\$(\d+\.\d+)\s*(?:\/|per|\s)(?:hour|hr)',  # $17.15-$18.25/hr
            r'\$(\d+)\s*(?:-|to)\s*\$(\d+)\s*(?:\/|per|\s)(?:hour|hr)',  # $17-$18/hr
            r'\$(\d+)\s*per hour to \$(\d+\.?\d*)', # $18 per hour to $19.25
            r'\$(\d+\.?\d*)\s*per hour to \$(\d+)', # $18.75 per hour to $19
            r'\$(\d+)\s*-\s*\$(\d+\.?\d*)', # $18 - $19.25
            r'\$(\d+\.?\d*)\s*-\s*\$(\d+)', # $18.75 - $19
            r'\$(\d+)\s*to \$(\d+\.?\d*)', # $18 to $19.25
            r'\$(\d+\.?\d*)\s*to \$(\d+)', # $18.75 to $19
            r'\$(\d+)\s*per hour to \$(\d+)', # $18 per hour to $19
            r'\$(\d+)\s*-\s*\$(\d+)', # $18 - $19
            r'\$(\d+)\s*to \$(\d+)', # $18 to $19
        ]
        single_patterns = [
            # More flexible patterns for hourly rates
            r'(\$\d+\.?\d*)\s*(?:\/|\s*\/\s*|\s+per\s*|\s+an\s*|\s+)\s*h(?:ou)?r(?:s|\b)',  # $18.00 per hour, $17.15/hr
            r'(\$\d+\.?\d*)\s*(?:\/|\s*\/\s*|\s+per\s*|\s+an\s*|\s+)\s*HR\b',               # $15.50/HR
            r'(\$\d+\.?\d*)\s*(?:\/|\s*\/\s*|\s+per\s*|\s+an\s*)\s*hour\b',                 # $19.50 an hour
            r'(\$\d+\.?\d*)\s*-\s*\$\d+\.?\d*\s*(?:\/|\s*\/\s*|\s+per\s*|\s+an\s*)\s*h(?:ou)?r', # $15.50-$18.50/hr

            # Basic dollar amount patterns (only if no hourly indicator found)
            r'(\$\d+\.\d+)',  # $17.15
            r'(\$\d+)',       # $17
            
            r'WAGE:\s*(\d+\.?\d*)\s*per hour',  # WAGE: 20.32 per hour
            r'WAGE:\s*(\d+)\s*per hour'  # WAGE: 20 per hour
        ]
        
        # First try min max range patterns
        for pattern in min_max_patterns:
            matches = re.findall(pattern, description)
            
            if matches:
                self.logger.info(f"Range pattern '{pattern}' found matches: {matches}")

                try:
                    min_rate = float(matches[0][0])
                    max_rate = float(matches[0][1])
                    hourly_rate = (min_rate + max_rate) / 2
                    annual_salary = round(hourly_rate * 40 * 52, 2)
                    self.logger.info(f"Found range hourly rate: ${hourly_rate}/hr -> annual: ${annual_salary}")
                    return annual_salary
                except (ValueError, TypeError) as e:
                    self.logger.info(f"Failed to parse range values: {matches[0]} - Error: {e}")
                    continue

        # Then try single value patterns
        for pattern in single_patterns:
            matches = re.findall(pattern, description)
            if matches:
                self.logger.info(f"Pattern '{pattern}' found matches: {matches}")
                try:
                    hourly_rate = float(matches[0].replace('$', '').replace(',', ''))
                    annual_salary = round(hourly_rate * 40 * 52, 2)
                    self.logger.info(f"Found single value hourly rate: ${hourly_rate}/hr -> annual: ${annual_salary}")
                    return annual_salary
                except (ValueError, TypeError) as e:
                    self.logger.info(f"Failed to parse single value: {matches[0]} - Error: {e}")
                    continue
        
        # If no matches found, log with better dollar amount detection
        all_amounts = re.findall(r'\$', description)
        if all_amounts:
            self.logger.info(f"Found $ but couldn't extract salary. Dollar amounts found: {['$' + amt for amt in all_amounts]}. Description: {description}")
        
        return None

    def extract_job_key(self, job_data):
        return job_data.get('req_id')
    
    def extract_job_url(self, job_data):
        """Extract job page URL from various possible locations in the data"""
        meta_data = job_data.get('meta_data', {})
        
        # Try different possible URL locations in order of preference
        url = (meta_data.get('canonical_url') or 
            job_data.get('apply_url') or 
            job_data.get('canonical_url'))
        
        return url if url else None
    
    def extract_zipcode(self, job_data):
        """Extract and validate zipcode from job data"""
        try:
            # Check multiple possible locations for zipcode
            zipcode = None
            
            # Check direct postal_code field
            if 'postal_code' in job_data:
                zipcode = job_data['postal_code']
            
            # Check in metadata/googlejobs/derivedInfo/locations/postalAddress
            elif 'meta_data' in job_data and 'googlejobs' in job_data['meta_data']:
                derived_info = job_data['meta_data']['googlejobs'].get('derivedInfo', {})
                locations = derived_info.get('locations', [])
                if locations:
                    postal_address = locations[0].get('postalAddress', {})
                    zipcode = postal_address.get('postalCode', '')

            if not zipcode or not isinstance(zipcode, str):
                return None
            
            # Check if it's a Canadian postal code (contains letters)
            if re.search(r'[A-Za-z]', zipcode):
                return None
            
            # For US zipcodes, take only the first 5 digits
            # This handles both regular (12345) and ZIP+4 (12345-6789) formats
            match = re.match(r'(\d{5})', zipcode)
            if match:
                return match.group(1)  # Return as string to preserve leading zeros
            
            return None
            
        except (ValueError, TypeError) as e:
            self.logger.error(f"Error extracting zipcode: {e}")
            return None

