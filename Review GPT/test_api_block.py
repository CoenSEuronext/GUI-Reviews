import requests
import socket
import ssl

def test_network_connectivity():
    # Test DNS resolution
    try:
        socket.gethostbyname('api.openai.com')
        print("DNS Resolution: Successful")
    except socket.gaierror:
        print("DNS Resolution: Failed")
        return False

    # Test direct OpenAI API endpoint connectivity
    try:
        response = requests.get('https://api.openai.com', timeout=5)
        print(f"HTTPS Connectivity: {response.status_code}")
        return response.status_code == 200
    except requests.RequestException as e:
        print(f"Connection Error: {e}")
        return False

def check_ssl_certificate():
    try:
        ssl_context = ssl.create_default_context()
        with socket.create_connection(('api.openai.com', 443)) as sock:
            with ssl_context.wrap_socket(sock, server_hostname='api.openai.com') as secure_sock:
                cert = secure_sock.getpeercert()
                print("SSL Certificate: Valid")
                return True
    except Exception as e:
        print(f"SSL Certificate Check Failed: {e}")
        return False

def main():
    print("OpenAI API Network Diagnostics:")
    network_ok = test_network_connectivity()
    ssl_ok = check_ssl_certificate()
    
    if network_ok and ssl_ok:
        print("No obvious network blocks detected")
    else:
        print("Potential network connectivity issues")

if __name__ == '__main__':
    main()