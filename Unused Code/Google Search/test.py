import requests
import time
import csv
from bs4 import BeautifulSoup
import os
import random
from urllib.parse import quote_plus
import winreg
import subprocess
import socket

class VPNAwareEmployeeVerifier:
    def __init__(self):
        self.session = requests.Session()
        self.setup_network_config()
        self.delay_range = (4, 8)  # Longer delays for stability
        self.processed_count = 0
        
    def setup_network_config(self):
        """Configure Python to use system proxy/VPN settings"""
        try:
            # Method 1: Use system proxy settings
            proxy_settings = self.get_system_proxy()
            if proxy_settings:
                self.session.proxies.update(proxy_settings)
                print(f"✅ Using system proxy: {proxy_settings}")
            
            # Method 2: Configure session for VPN compatibility
            self.session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.2210.121',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept-Encoding': 'gzip, deflate, br',
                'DNT': '1',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1',
                'Sec-Fetch-Dest': 'document',
                'Sec-Fetch-Mode': 'navigate',
                'Sec-Fetch-Site': 'none',
                'Cache-Control': 'max-age=0'
            })
            
            # Method 3: Force IPv4 (some VPNs have IPv6 issues)
            socket.getaddrinfo = self.force_ipv4_getaddrinfo
            
            # Method 4: Increase timeouts for VPN delays
            self.session.timeout = 45
            
        except Exception as e:
            print(f"⚠️ Network config warning: {e}")
    
    def force_ipv4_getaddrinfo(self, host, port, family=0, type=0, proto=0, flags=0):
        """Force IPv4 resolution to avoid VPN IPv6 issues"""
        return socket.getaddrinfo(host, port, socket.AF_INET, type, proto, flags)
    
    def get_system_proxy(self):
        """Extract Windows system proxy settings"""
        try:
            # Read Windows proxy settings from registry
            with winreg.OpenKey(winreg.HKEY_CURRENT_USER, 
                              r'Software\Microsoft\Windows\CurrentVersion\Internet Settings') as key:
                proxy_enable = winreg.QueryValueEx(key, 'ProxyEnable')[0]
                
                if proxy_enable:
                    proxy_server = winreg.QueryValueEx(key, 'ProxyServer')[0]
                    
                    if ':' in proxy_server:
                        proxy_url = f"http://{proxy_server}"
                        return {
                            'http': proxy_url,
                            'https': proxy_url
                        }
        except:
            pass
        return None
    
    def test_connectivity(self):
        """Test if we can reach external sites"""
        test_urls = [
            'https://httpbin.org/ip',  # Simple IP check
            'https://www.bing.com',
            'https://duckduckgo.com'
        ]
        
        for url in test_urls:
            try:
                print(f"Testing connectivity to {url}...")
                response = self.session.get(url, timeout=30)
                
                if response.status_code == 200:
                    print(f"✅ Successfully connected to {url}")
                    if 'httpbin' in url:
                        data = response.json()
                        print(f"   Your IP: {data.get('origin', 'Unknown')}")
                    return True
                else:
                    print(f"⚠️ {url} returned status {response.status_code}")
                    
            except Exception as e:
                print(f"❌ Failed to connect to {url}: {str(e)[:100]}...")
                
        return False
    
    def search_alternative_engines(self, first_name, last_name, company):
        """Try multiple search engines with VPN-friendly approaches"""
        
        # Strategy 1: Try DuckDuckGo (often more VPN-friendly)
        try:
            result = self.search_duckduckgo_html(first_name, last_name, company)
            if result is not None:
                return result
        except Exception as e:
            print(f"    DuckDuckGo failed: {str(e)[:50]}...")
        
        # Strategy 2: Try Startpage (privacy-focused, VPN-friendly)
        try:
            result = self.search_startpage(first_name, last_name, company)
            if result is not None:
                return result
        except Exception as e:
            print(f"    Startpage failed: {str(e)[:50]}...")
        
        # Strategy 3: Try Bing with different approach
        try:
            result = self.search_bing_mobile(first_name, last_name, company)
            if result is not None:
                return result
        except Exception as e:
            print(f"    Bing mobile failed: {str(e)[:50]}...")
        
        return False
    
    def search_duckduckgo_html(self, first_name, last_name, company):
        """Search DuckDuckGo HTML version (more VPN-compatible)"""
        try:
            query = f'"{first_name} {last_name}" "{company}"'
            encoded_query = quote_plus(query)
            
            # Use DuckDuckGo HTML version (no JavaScript required)
            url = f"https://html.duckduckgo.com/html/?q={encoded_query}"
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for results in DuckDuckGo HTML format
                results = soup.find_all(['a', 'span', 'div'], limit=20)
                
                for result in results:
                    text = result.get_text().lower()
                    
                    if (first_name.lower() in text and 
                        last_name.lower() in text and 
                        company.lower() in text):
                        
                        # Check for employment indicators
                        employment_keywords = [
                            'linkedin', 'employee', 'works at', 'consultant',
                            'manager', 'director', 'senior', 'lead', 'engineer'
                        ]
                        
                        if any(keyword in text for keyword in employment_keywords):
                            return True
                            
            return False
            
        except Exception as e:
            print(f"    DuckDuckGo HTML error: {e}")
            return None
    
    def search_startpage(self, first_name, last_name, company):
        """Search Startpage (privacy-focused search engine)"""
        try:
            query = f'"{first_name} {last_name}" "{company}"'
            encoded_query = quote_plus(query)
            
            url = f"https://www.startpage.com/sp/search?query={encoded_query}"
            
            response = self.session.get(url, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Look for search results
                results = soup.find_all(['h3', 'p', 'span'], limit=15)
                
                for result in results:
                    text = result.get_text().lower()
                    
                    if (first_name.lower() in text and 
                        last_name.lower() in text and 
                        company.lower() in text):
                        
                        employment_keywords = [
                            'linkedin', 'employee', 'works at', 'consultant',
                            'manager', 'director', 'profile'
                        ]
                        
                        if any(keyword in text for keyword in employment_keywords):
                            return True
                            
            return False
            
        except Exception as e:
            print(f"    Startpage error: {e}")
            return None
    
    def search_bing_mobile(self, first_name, last_name, company):
        """Try Bing mobile version (sometimes more accessible)"""
        try:
            # Use mobile user agent
            mobile_headers = self.session.headers.copy()
            mobile_headers['User-Agent'] = 'Mozilla/5.0 (iPhone; CPU iPhone OS 14_7_1 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Mobile/15E148 Safari/604.1'
            
            query = f'"{first_name} {last_name}" "{company}"'
            encoded_query = quote_plus(query)
            
            url = f"https://m.bing.com/search?q={encoded_query}"
            
            response = self.session.get(url, headers=mobile_headers, timeout=30)
            
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                
                # Mobile Bing has different structure
                results = soup.find_all(['div', 'span', 'a'], limit=20)
                
                for result in results:
                    text = result.get_text().lower()
                    
                    if (first_name.lower() in text and 
                        last_name.lower() in text and 
                        company.lower() in text):
                        
                        employment_keywords = [
                            'linkedin', 'employee', 'works at', 'consultant',
                            'manager', 'director', 'senior'
                        ]
                        
                        if any(keyword in text for keyword in employment_keywords):
                            return True
                            
            return False
            
        except Exception as e:
            print(f"    Bing mobile error: {e}")
            return None
    
    def verify_employment(self, first_name, last_name, company):
        """
        Main verification function with VPN-aware searching
        Returns: 'YES' or 'NO'
        """
        print(f"Verifying: {first_name} {last_name} at {company}")
        
        try:
            result = self.search_alternative_engines(first_name, last_name, company)
            
            if result is True:
                print(f"  → Result: YES - Employment relationship found")
                return 'YES'
            else:
                print(f"  → Result: NO - No clear employment relationship found")
                return 'NO'
                
        except Exception as e:
            print(f"  → Error during verification: {e}")
            return 'NO'
        
        finally:
            # Add delay to avoid overwhelming the network/VPN
            delay = random.uniform(*self.delay_range)
            print(f"  → Waiting {delay:.1f} seconds...")
            time.sleep(delay)
    
    def process_norconsult_csv(self):
        """Process the NorConsult CSV file with automated verification"""
        input_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Google Search\Input\Names NorConsult.csv"
        output_path = r"C:\Users\CSonneveld\OneDrive - Euronext\Documents\Projects\Google Search\Output\NorConsult_Verification_Results_Auto.csv"
        
        print("🔍 Automated Employee Verification (VPN-Aware)")
        print("=" * 60)
        
        # Test connectivity first
        if not self.test_connectivity():
            print("❌ Cannot establish external connectivity.")
            print("   This might be due to:")
            print("   - VPN not routing Python traffic")
            print("   - Corporate firewall blocking non-browser requests")
            print("   - Need for specific proxy configuration")
            return None
        
        if not os.path.exists(input_path):
            print(f"Error: Input file not found at {input_path}")
            return
        
        results = []
        
        try:
            with open(input_path, 'r', encoding='utf-8') as file:
                # Handle BOM and detect delimiter
                content = file.read()
                if content.startswith('\ufeff'):
                    content = content[1:]
                
                lines = content.strip().split('\n')
                delimiter = ','
                if '\t' in lines[0]:
                    delimiter = '\t'
                elif ';' in lines[0]:
                    delimiter = ';'
                
                reader = csv.DictReader(lines, delimiter=delimiter)
                
                # Clean column names
                if reader.fieldnames:
                    fieldnames = [col.lstrip('\ufeff') for col in reader.fieldnames]
                    reader.fieldnames = fieldnames
                
                print(f"Detected columns: {reader.fieldnames}")
                print(f"Starting automated verification...")
                print("=" * 60)
                
                for i, row in enumerate(reader, 1):
                    first_name = (row.get('first_name') or row.get('First Name') or 
                                row.get('FirstName') or row.get('Name') or '').strip()
                    
                    last_name = (row.get('last_name') or row.get('Last Name') or 
                               row.get('LastName') or row.get('Surname') or '').strip()
                    
                    # Handle single name column
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
                    
                    self.processed_count += 1
                    
                    # Save progress every 5 records
                    if self.processed_count % 5 == 0:
                        self.save_results(results, output_path)
                        print(f"  💾 Progress saved ({self.processed_count} records)")
        
        except Exception as e:
            print(f"Error processing CSV: {e}")
            return results
        
        # Save final results
        self.save_results(results, output_path)
        
        print(f"\n" + "="*60)
        print(f"✅ Verification complete!")
        print(f"📄 Results saved to: {output_path}")
        print(f"📊 Processed: {self.processed_count} records")
        
        # Summary statistics
        if results:
            yes_count = sum(1 for r in results if r['works_at_company'] == 'YES')
            no_count = sum(1 for r in results if r['works_at_company'] == 'NO')
            
            print(f"\n📈 Summary:")
            print(f"   YES: {yes_count} ({yes_count/len(results)*100:.1f}%)")
            print(f"   NO: {no_count} ({no_count/len(results)*100:.1f}%)")
        
        return results
    
    def save_results(self, results, output_path):
        """Save results to CSV file"""
        try:
            # Create output directory if needed
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            with open(output_path, 'w', newline='', encoding='utf-8') as file:
                fieldnames = ['first_name', 'last_name', 'company', 'works_at_company']
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(results)
                
        except Exception as e:
            print(f"Error saving results: {e}")

def main():
    verifier = VPNAwareEmployeeVerifier()
    results = verifier.process_norconsult_csv()
    
    if results:
        print(f"\n🎉 Process completed successfully!")
    else:
        print(f"\n⚠️ Process completed with issues - check connectivity")

if __name__ == "__main__":
    main()