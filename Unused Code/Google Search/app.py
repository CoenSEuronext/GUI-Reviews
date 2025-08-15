import requests
import time
import csv
from bs4 import BeautifulSoup
import json
from urllib.parse import quote_plus
import re
import os

class EmployeeVerifier:
    def __init__(self, api_key, search_engine_id):
        self.api_key = api_key
        self.search_engine_id = search_engine_id
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
        self.rate_limit_delay = 1.5  # seconds between requests
        self.daily_quota_used = 0
        self.max_daily_quota = 100  # Google's free tier limit
        
    def search_google(self, first_name, last_name, company):
        """Search Google for name + company combination"""
        if self.daily_quota_used >= self.max_daily_quota:
            print(f"Daily quota reached ({self.max_daily_quota}). Consider upgrading API plan.")
            return None
            
        # Try multiple search variations for better results
        queries = [
            f'"{first_name} {last_name}" "{company}"',
            f'"{first_name} {last_name}" site:linkedin.com "{company}"',
            f'"{first_name} {last_name}" "{company}" employee OR consultant OR manager'
        ]
        
        for query in queries:
            encoded_query = quote_plus(query)
            search_url = f"https://www.googleapis.com/customsearch/v1?key={self.api_key}&cx={self.search_engine_id}&q={encoded_query}"
            
            try:
                response = self.session.get(search_url)
                self.daily_quota_used += 1
                
                if response.status_code == 200:
                    data = response.json()
                    
                    if 'items' in data:
                        for item in data['items']:
                            title = item.get('title', '').lower()
                            snippet = item.get('snippet', '').lower()
                            url = item.get('link', '').lower()
                            
                            # Strong indicators of employment
                            if self._is_strong_employment_match(first_name, last_name, company, title, snippet, url):
                                return True
                
                time.sleep(self.rate_limit_delay)
                
            except Exception as e:
                print(f"Google search error for {first_name} {last_name}: {e}")
                time.sleep(self.rate_limit_delay)
                
        return False
    
    def _is_strong_employment_match(self, first_name, last_name, company, title, snippet, url):
        """Determine if search result strongly indicates employment"""
        name_full = f"{first_name} {last_name}".lower()
        company_lower = company.lower()
        
        # Strong employment indicators
        employment_keywords = [
            'employee', 'works at', 'senior', 'manager', 'director', 
            'consultant', 'engineer', 'analyst', 'specialist', 'lead'
        ]
        
        # Check if both name and company appear together with employment context
        text_to_check = f"{title} {snippet}".lower()
        
        has_name = (first_name.lower() in text_to_check and last_name.lower() in text_to_check)
        has_company = company_lower in text_to_check
        has_employment_context = any(keyword in text_to_check for keyword in employment_keywords)
        
        # LinkedIn profiles are strong indicators
        if 'linkedin.com' in url and has_name and has_company:
            return True
            
        # Company domain with name is strong indicator
        if company_lower.replace(' ', '').replace('.', '') in url and has_name:
            return True
            
        # All three conditions together
        if has_name and has_company and has_employment_context:
            return True
            
        return False
    
    def search_linkedin(self, first_name, last_name, company):
        """Search LinkedIn (requires LinkedIn API access or scraping)"""
        # This would require LinkedIn API or careful scraping
        # LinkedIn has strict anti-scraping measures
        query = f"{first_name} {last_name} {company}"
        
        # Placeholder - you'd need to implement actual LinkedIn search
        # using their API or a service like ScrapFly/Bright Data
        return None
    
    def search_company_website(self, first_name, last_name, company, company_domain=None):
        """Search company's own website/directory"""
        if not company_domain:
            # Try to find company domain
            company_domain = self.find_company_domain(company)
        
        if company_domain:
            search_urls = [
                f"https://www.google.com/search?q=site:{company_domain} \"{first_name} {last_name}\"",
                f"https://{company_domain}/team",
                f"https://{company_domain}/about",
                f"https://{company_domain}/people"
            ]
            
            for url in search_urls:
                try:
                    if 'google.com' in url:
                        # Google site search
                        response = self.session.get(url)
                        if response.status_code == 200:
                            content = response.text.lower()
                            if first_name.lower() in content and last_name.lower() in content:
                                return True
                    else:
                        # Direct company page search
                        response = self.session.get(url, timeout=10)
                        if response.status_code == 200:
                            soup = BeautifulSoup(response.content, 'html.parser')
                            text = soup.get_text().lower()
                            if first_name.lower() in text and last_name.lower() in text:
                                return True
                                
                except Exception as e:
                    continue
                    
        return False
    
    def find_company_domain(self, company):
        """Try to find company's main domain"""
        query = f'"{company}" site:com OR site:org'
        # This would use search to find the main company website
        # Implementation depends on your search method
        return None
    
    def verify_employment(self, first_name, last_name, company):
        """
        Main verification function
        Returns: 'YES' or 'NO'
        """
        print(f"Verifying: {first_name} {last_name} at {company}")
        
        # Primary search method
        google_result = self.search_google(first_name, last_name, company)
        
        if google_result is True:
            return 'YES'
        elif google_result is False:
            return 'NO'
        else:
            # If search failed, return NO (conservative approach)
            return 'NO'
    
    def process_norconsult_csv(self):
        """Process the specific NorConsult CSV file"""
        input_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Google Search\Input\Names NorConsult.csv"
        output_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Google Search\Output\NorConsult_Verification_Results.csv"
        
        if not os.path.exists(input_path):
            print(f"Error: Input file not found at {input_path}")
            return
        
        results = []
        processed_count = 0
        
        try:
            with open(input_path, 'r', encoding='utf-8') as file:
                # Try to detect the CSV structure
                sample = file.read(1024)
                file.seek(0)
                
                # Detect delimiter
                delimiter = ','
                if '\t' in sample:
                    delimiter = '\t'
                elif ';' in sample:
                    delimiter = ';'
                
                reader = csv.DictReader(file, delimiter=delimiter)
                
                # Clean BOM characters from column names
                if reader.fieldnames:
                    reader.fieldnames = [col.lstrip('\ufeff') for col in reader.fieldnames]
                
                # Print detected columns for verification
                print(f"Detected columns: {reader.fieldnames}")
                
                for i, row in enumerate(reader, 1):
                    # Handle BOM character and try different possible column names
                    first_name = (row.get('first_name') or row.get('\ufefffirst_name') or 
                                row.get('First Name') or row.get('FirstName') or 
                                row.get('Name') or '').strip()
                    
                    last_name = (row.get('last_name') or row.get('Last Name') or 
                               row.get('LastName') or row.get('Surname') or '').strip()
                    
                    # If name is in single column, try to split
                    if not last_name and first_name:
                        name_parts = first_name.split()
                        if len(name_parts) >= 2:
                            first_name = name_parts[0]
                            last_name = ' '.join(name_parts[1:])
                    
                    company = (row.get('company') or row.get('Company') or 
                             row.get('Organization') or 'NorConsult').strip()
                    
                    if not first_name or not last_name:
                        print(f"Row {i}: Missing name data, skipping")
                        continue
                    
                    result = self.verify_employment(first_name, last_name, company)
                    
                    results.append({
                        'first_name': first_name,
                        'last_name': last_name,
                        'company': company,
                        'works_at_company': result
                    })
                    
                    processed_count += 1
                    
                    # Progress update
                    if processed_count % 10 == 0:
                        print(f"Processed {processed_count} records...")
                        
                    # Quota check
                    if self.daily_quota_used >= self.max_daily_quota:
                        print(f"Reached daily API quota. Processed {processed_count} out of total records.")
                        break
        
        except Exception as e:
            print(f"Error reading CSV: {e}")
            return
        
        # Save results
        try:
            with open(output_path, 'w', newline='', encoding='utf-8') as file:
                fieldnames = ['first_name', 'last_name', 'company', 'works_at_company']
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
            
            print(f"\nVerification complete!")
            print(f"Results saved to: {output_path}")
            print(f"Processed: {processed_count} records")
            print(f"API calls used: {self.daily_quota_used}")
            
            # Summary statistics
            yes_count = sum(1 for r in results if r['works_at_company'] == 'YES')
            no_count = sum(1 for r in results if r['works_at_company'] == 'NO')
            
            print(f"\nSummary:")
            print(f"YES: {yes_count} ({yes_count/len(results)*100:.1f}%)")
            print(f"NO: {no_count} ({no_count/len(results)*100:.1f}%)")
            
        except Exception as e:
            print(f"Error saving results: {e}")
        
        return results

# Setup and Usage Instructions
"""
SETUP STEPS:

1. Get Google Custom Search API credentials:
   - Go to https://console.cloud.google.com/
   - Create a new project or select existing
   - Enable "Custom Search API"
   - Create API key in "Credentials"
   - Create Custom Search Engine at https://cse.google.com/
   - Get your Search Engine ID

2. Install required packages:
   pip install requests beautifulsoup4

3. Update the credentials below and run:
"""

# Configuration
GOOGLE_API_KEY = "AIzaSyDNLvvFZfnFh0HCx6NRCIsuXNmy1yjIMZU"  # Replace with your actual API key
SEARCH_ENGINE_ID = "207094f79b5f04568"     # Replace with your Custom Search Engine ID

def main():
    # Initialize verifier with your credentials
    verifier = EmployeeVerifier(GOOGLE_API_KEY, SEARCH_ENGINE_ID)
    
    # Process your specific file
    print("Starting NorConsult employee verification...")
    print("This will search for each person at NorConsult and provide YES/NO answers.")
    print("=" * 60)
    
    results = verifier.process_norconsult_csv()
    
    if results:
        print(f"\nVerification completed! Check the results file.")
    else:
        print("\nVerification failed. Please check the error messages above.")

if __name__ == "__main__":
    main()