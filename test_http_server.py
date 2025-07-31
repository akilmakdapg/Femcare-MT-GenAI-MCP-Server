#!/usr/bin/env python3
"""
Test script for the Cosmos DB MCP HTTP Server
"""
import asyncio
import aiohttp
import json

async def test_server():
    """Test the HTTP MCP server endpoints"""
    base_url = "http://localhost:8000"
    
    async with aiohttp.ClientSession() as session:
        try:
            # Test server info
            print("Testing server info...")
            async with session.get(f"{base_url}/") as response:
                data = await response.json()
                print(f"Server info: {json.dumps(data, indent=2)}")
            
            # Test health check
            print("\nTesting health check...")
            async with session.get(f"{base_url}/health") as response:
                data = await response.json()
                print(f"Health check: {json.dumps(data, indent=2)}")
            
            # Test list resources
            print("\nTesting list resources...")
            async with session.get(f"{base_url}/mcp/resources") as response:
                data = await response.json()
                print(f"Resources: {json.dumps(data, indent=2)}")
            
            # Test list tools
            print("\nTesting list tools...")
            async with session.get(f"{base_url}/mcp/tools") as response:
                data = await response.json()
                print(f"Tools: {len(data.get('tools', []))} tools available")
                for tool in data.get('tools', []):
                    print(f"  - {tool['name']}: {tool['description']}")
            
            # Test query documents tool
            print("\nTesting query documents tool...")
            query_data = {
                "arguments": {
                    "query": "SELECT TOP 5 * FROM c"
                }
            }
            async with session.post(
                f"{base_url}/mcp/tools/query_documents",
                json=query_data
            ) as response:
                data = await response.json()
                print(f"Query result status: {response.status}")
                if response.status == 200:
                    result = data.get('result', [])
                    if result:
                        print(f"Query successful: {len(result)} items in response")
                    else:
                        print("Query returned empty result")
                else:
                    print(f"Query error: {data}")
            
        except aiohttp.ClientError as e:
            print(f"Connection error: {e}")
            print("Make sure the server is running on http://localhost:8000")
        except Exception as e:
            print(f"Test error: {e}")

if __name__ == "__main__":
    print("Testing Cosmos DB MCP HTTP Server...")
    print("Make sure the server is running before running this test.")
    print("Start server with: python main.py")
    print("-" * 50)
    
    asyncio.run(test_server())
