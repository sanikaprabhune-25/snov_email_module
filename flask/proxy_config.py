"""
Proxy rotation configuration and management
"""
import random
import asyncio
from typing import List, Dict, Optional
import aiohttp
import time

class ProxyRotator:
    def __init__(self):
        # Add your proxy list here - format: "ip:port" or "ip:port:username:password"
        self.proxy_list = [
            # Free proxies (replace with your premium proxies)
            "8.210.83.33:1080",
            "47.74.152.29:8888", 
            "43.134.234.74:21127",
            "103.149.162.194:80",
            "80.48.119.28:8080",
            "185.32.6.129:8090",
            "194.182.163.117:3128",
            "103.127.1.130:80",
            "181.78.105.157:999",
            "190.61.88.147:8080",
            # Add more proxies here
        ]
        
        self.current_index = 0
        self.failed_proxies = set()
        self.proxy_stats = {}
        self.last_rotation_time = 0
        self.rotation_interval = 300  # Rotate every 5 minutes
        
    def get_next_proxy(self) -> Optional[Dict[str, str]]:
        """Get the next working proxy in rotation"""
        if not self.proxy_list:
            return None
            
        # Check if it's time to rotate or if current proxy failed
        current_time = time.time()
        should_rotate = (current_time - self.last_rotation_time) > self.rotation_interval
        
        if should_rotate:
            self.current_index = (self.current_index + 1) % len(self.proxy_list)
            self.last_rotation_time = current_time
            
        # Find next working proxy
        attempts = 0
        while attempts < len(self.proxy_list):
            proxy_string = self.proxy_list[self.current_index]
            
            if proxy_string not in self.failed_proxies:
                return self._parse_proxy(proxy_string)
                
            # Move to next proxy if current one failed
            self.current_index = (self.current_index + 1) % len(self.proxy_list)
            attempts += 1
            
        # If all proxies failed, reset failed list and try again
        self.failed_proxies.clear()
        return self._parse_proxy(self.proxy_list[self.current_index])
    
    def _parse_proxy(self, proxy_string: str) -> Dict[str, str]:
        """Parse proxy string into Playwright format"""
        parts = proxy_string.split(':')
        
        if len(parts) == 2:
            # No authentication
            return {
                "server": f"http://{parts[0]}:{parts[1]}"
            }
        elif len(parts) == 4:
            # With authentication
            return {
                "server": f"http://{parts[0]}:{parts[1]}",
                "username": parts[2],
                "password": parts[3]
            }
        else:
            raise ValueError(f"Invalid proxy format: {proxy_string}")
    
    def mark_proxy_failed(self, proxy_config: Dict[str, str]):
        """Mark a proxy as failed"""
        server = proxy_config.get("server", "")
        # Extract IP:PORT from server URL
        if "://" in server:
            proxy_addr = server.split("://")[1]
            for proxy_string in self.proxy_list:
                if proxy_addr in proxy_string:
                    self.failed_proxies.add(proxy_string)
                    print(f"Marked proxy as failed: {proxy_string}")
                    break
    
    async def test_proxy(self, proxy_config: Dict[str, str]) -> bool:
        """Test if a proxy is working"""
        try:
            connector = aiohttp.TCPConnector()
            timeout = aiohttp.ClientTimeout(total=10)
            
            proxy_url = proxy_config.get("server")
            auth = None
            
            if "username" in proxy_config and "password" in proxy_config:
                auth = aiohttp.BasicAuth(
                    proxy_config["username"], 
                    proxy_config["password"]
                )
            
            async with aiohttp.ClientSession(
                connector=connector, 
                timeout=timeout
            ) as session:
                async with session.get(
                    "http://httpbin.org/ip", 
                    proxy=proxy_url,
                    proxy_auth=auth
                ) as response:
                    if response.status == 200:
                        data = await response.json()
                        print(f"Proxy test successful. IP: {data.get('origin')}")
                        return True
                    return False
                    
        except Exception as e:
            print(f"Proxy test failed: {e}")
            return False
    
    def get_random_proxy(self) -> Optional[Dict[str, str]]:
        """Get a random proxy from the list"""
        if not self.proxy_list:
            return None
            
        available_proxies = [p for p in self.proxy_list if p not in self.failed_proxies]
        if not available_proxies:
            # Reset failed list if all proxies failed
            self.failed_proxies.clear()
            available_proxies = self.proxy_list
            
        proxy_string = random.choice(available_proxies)
        return self._parse_proxy(proxy_string)
    
    def add_proxy(self, proxy_string: str):
        """Add a new proxy to the rotation"""
        if proxy_string not in self.proxy_list:
            self.proxy_list.append(proxy_string)
            print(f"Added new proxy: {proxy_string}")
    
    def remove_proxy(self, proxy_string: str):
        """Remove a proxy from rotation"""
        if proxy_string in self.proxy_list:
            self.proxy_list.remove(proxy_string)
            self.failed_proxies.discard(proxy_string)
            print(f"Removed proxy: {proxy_string}")
    
    def get_stats(self) -> Dict:
        """Get proxy rotation statistics"""
        return {
            "total_proxies": len(self.proxy_list),
            "failed_proxies": len(self.failed_proxies),
            "current_proxy_index": self.current_index,
            "current_proxy": self.proxy_list[self.current_index] if self.proxy_list else None,
            "rotation_interval": self.rotation_interval
        }

# Global proxy rotator instance
proxy_rotator = ProxyRotator()
