import os
import asyncio
import logging
from typing import Optional
from dataclasses import dataclass

from azure.cosmos.aio import CosmosClient
from dotenv import load_dotenv
from pydantic import AnyUrl

from mcp.server.models import InitializationOptions
from mcp.server import NotificationOptions, Server
from mcp.types import (
    Resource,
    Tool,
    TextContent
)
import mcp.types as types
from mcp.server.session import ServerSession
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
import uvicorn

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class CosmosConfig:
    """Configuration for Azure Cosmos DB connection"""
    endpoint: str
    key: str
    database_name: str
    container_name: str
    consistency_level: str = "Session"
    connection_mode: str = "Gateway"

@dataclass
class ServerConfig:
    """Configuration for HTTP server"""
    host: str = "localhost"
    port: int = 8000
    log_level: str = "info"

class CosmosDBMCPServer:
    """MCP Server for Azure Cosmos DB operations"""
    
    def __init__(self):
        self.config = self._load_config()
        self.server_config = self._load_server_config()
        self.cosmos_client: Optional[CosmosClient] = None
        self.database = None
        self.container = None
        self.server = Server("cosmosdb-mcp-server")
        self.app = FastAPI(title="Cosmos DB MCP Server", version="1.0.0")
        self._setup_handlers()
        self._setup_http_routes()
    
    def _load_config(self) -> CosmosConfig:
        """Load configuration from environment variables"""
        return CosmosConfig(
            endpoint=os.getenv("COSMOS_ENDPOINT", ""),
            key=os.getenv("COSMOS_KEY", ""),
            database_name=os.getenv("COSMOS_DATABASE_NAME", ""),
            container_name=os.getenv("COSMOS_CONTAINER_NAME", ""),
        )
    
    def _load_server_config(self) -> ServerConfig:
        """Load HTTP server configuration from environment variables"""
        return ServerConfig(
            host=os.getenv("SERVER_HOST", "localhost"),
            port=int(os.getenv("SERVER_PORT", "8000")),
            log_level=os.getenv("LOG_LEVEL", "info")
        )
    
    async def _initialize_cosmos_client(self):
        """Initialize Azure Cosmos DB client and verify connection"""
        try:
            self.cosmos_client = CosmosClient(
                url=self.config.endpoint,
                credential=self.config.key
            )
            
            # Get database reference
            self.database = self.cosmos_client.get_database_client(self.config.database_name)
            
            # Get container reference
            self.container = self.database.get_container_client(self.config.container_name)
            
            # Test connection
            await self.database.read()
            logger.info(f"Successfully connected to Cosmos DB: {self.config.database_name}")
            
        except Exception as e:
            logger.error(f"Failed to initialize Cosmos DB client: {str(e)}")
            raise
    
    def _setup_handlers(self):
        """Setup MCP server handlers"""
        
        @self.server.list_resources()
        async def handle_list_resources() -> list[Resource]:
            """List available Cosmos DB resources"""
            return [
                Resource(
                    uri=AnyUrl("cosmosdb://database"),
                    name="Database Info",
                    description="Information about the connected Cosmos DB database",
                    mimeType="application/json"
                ),
                Resource(
                    uri=AnyUrl("cosmosdb://container"),
                    name="Container Info", 
                    description="Information about the connected Cosmos DB container",
                    mimeType="application/json"
                ),
                Resource(
                    uri=AnyUrl("cosmosdb://documents"),
                    name="Documents",
                    description="Access to documents in the container",
                    mimeType="application/json"
                )
            ]
        
        @self.server.read_resource()
        async def handle_read_resource(uri: AnyUrl) -> str:
            """Read a specific Cosmos DB resource"""
            if not self.cosmos_client:
                await self._initialize_cosmos_client()
            
            if not self.database or not self.container:
                raise RuntimeError("Database or container not initialized")
            
            uri_str = str(uri)
            if uri_str == "cosmosdb://database":
                db_info = await self.database.read()
                return f"Database: {db_info['id']}\nCreated: {db_info.get('_ts', 'N/A')}"
            
            elif uri_str == "cosmosdb://container":
                container_info = await self.container.read()
                return f"Container: {container_info['id']}\nPartition Key: {container_info.get('partitionKey', 'N/A')}"
            
            elif uri_str == "cosmosdb://documents":
                # Return a sample of documents
                items = []
                async for item in self.container.query_items(
                    query="SELECT TOP 10 * FROM c"
                ):
                    items.append(item)
                return f"Sample documents (first 10):\n{items}"
            
            else:
                raise ValueError(f"Unknown resource URI: {uri_str}")
        
        @self.server.list_tools()
        async def handle_list_tools() -> list[Tool]:
            """List available Cosmos DB tools"""
            return [
                Tool(
                    name="query_documents",
                    description="Execute a SQL query against Cosmos DB documents",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "query": {
                                "type": "string",
                                "description": "SQL query to execute"
                            },
                            "parameters": {
                                "type": "array",
                                "items": {"type": "object"},
                                "description": "Optional query parameters"
                            },
                            "cross_partition": {
                                "type": "boolean",
                                "description": "Enable cross-partition query",
                                "default": True
                            }
                        },
                        "required": ["query"]
                    }
                ),
                Tool(
                    name="read_document",
                    description="Read a specific document by ID",
                    inputSchema={
                        "type": "object",
                        "properties": {
                            "document_id": {
                                "type": "string",
                                "description": "Document ID to read"
                            },
                            "partition_key": {
                                "type": "string",
                                "description": "Partition key value"
                            }
                        },
                        "required": ["document_id", "partition_key"]
                    }
                ),             
                Tool(
                    name="get_container_statistics",
                    description="Get statistics about the container",
                    inputSchema={
                        "type": "object",
                        "properties": {},
                        "required": []
                    }
                )
            ]
        
        @self.server.call_tool()
        async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
            """Handle tool calls"""
            if not self.cosmos_client:
                await self._initialize_cosmos_client()
            
            try:
                if name == "query_documents":
                    result = await self._query_documents(arguments)
                elif name == "read_document":
                    result = await self._read_document(arguments)
                elif name == "get_container_statistics":
                    result = await self._get_container_statistics()
                else:
                    raise ValueError(f"Unknown tool: {name}")
                
                return [TextContent(type="text", text=str(result))]
                
            except Exception as e:
                error_msg = f"Error executing {name}: {str(e)}"
                logger.error(error_msg)
                return [TextContent(type="text", text=error_msg)]
    
    async def _query_documents(self, arguments: dict) -> dict:
        """Execute a query against Cosmos DB"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        query = arguments["query"]
        parameters = arguments.get("parameters", [])
        cross_partition = arguments.get("cross_partition", True)
        
        items = []
        async for item in self.container.query_items(
            query=query,
            parameters=parameters
        ):
            items.append(item)
        
        return {
            "query": query,
            "result_count": len(items),
            "documents": items
        }
        
    async def _read_document(self, arguments: dict) -> dict:
        """Read a document by ID"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        document_id = arguments["document_id"]
        partition_key = arguments["partition_key"]
        
        document = await self.container.read_item(
            item=document_id,
            partition_key=partition_key
        )
        
        return {
            "status": "found",
            "document": document
        }
        
    async def _get_container_statistics(self) -> dict:
        """Get container statistics"""
        if not self.container:
            raise RuntimeError("Container not initialized")
            
        try:
            # Get container properties
            container_props = await self.container.read()
            
            # Count documents (this might be expensive for large containers)
            count_query = "SELECT VALUE COUNT(1) FROM c"
            count_result = []
            async for item in self.container.query_items(
                query=count_query
            ):
                count_result.append(item)
            
            document_count = count_result[0] if count_result else 0
            
            return {
                "container_id": container_props["id"],
                "partition_key": container_props.get("partitionKey", {}),
                "document_count": document_count,
                "indexing_policy": container_props.get("indexingPolicy", {}),
                "created_timestamp": container_props.get("_ts")
            }
            
        except Exception as e:
                return {
                    "error": f"Failed to get statistics: {str(e)}"
                }
    
    def _setup_http_routes(self):
        """Setup HTTP routes for the MCP server"""
        
        @self.app.get("/")
        async def root():
            return {
                "name": "Cosmos DB MCP Server",
                "version": "1.0.0",
                "description": "Model Context Protocol server for Azure Cosmos DB"
            }
        
        @self.app.get("/health")
        async def health_check():
            try:
                if not self.cosmos_client:
                    await self._initialize_cosmos_client()
                if self.database:
                    await self.database.read()
                return {"status": "healthy", "database": "connected"}
            except Exception as e:
                return {"status": "unhealthy", "error": str(e)}
        
        @self.app.get("/mcp/resources")
        async def list_resources():
            try:
                # Get resources directly from handler
                resources = [
                    {
                        "uri": "cosmosdb://database",
                        "name": "Database Info",
                        "description": "Information about the connected Cosmos DB database",
                        "mimeType": "application/json"
                    },
                    {
                        "uri": "cosmosdb://container",
                        "name": "Container Info", 
                        "description": "Information about the connected Cosmos DB container",
                        "mimeType": "application/json"
                    },
                    {
                        "uri": "cosmosdb://documents",
                        "name": "Documents",
                        "description": "Access to documents in the container",
                        "mimeType": "application/json"
                    }
                ]
                return {"resources": resources}
            except Exception as e:
                return JSONResponse(content={"error": str(e)}, status_code=500)
        
        @self.app.get("/mcp/resources/{resource_path:path}")
        async def read_resource(resource_path: str):
            try:
                if not self.cosmos_client:
                    await self._initialize_cosmos_client()
                
                if not self.database or not self.container:
                    raise RuntimeError("Database or container not initialized")
                
                uri_str = f"cosmosdb://{resource_path}"
                if uri_str == "cosmosdb://database":
                    db_info = await self.database.read()
                    content = f"Database: {db_info['id']}\nCreated: {db_info.get('_ts', 'N/A')}"
                elif uri_str == "cosmosdb://container":
                    container_info = await self.container.read()
                    content = f"Container: {container_info['id']}\nPartition Key: {container_info.get('partitionKey', 'N/A')}"
                elif uri_str == "cosmosdb://documents":
                    items = []
                    async for item in self.container.query_items(query="SELECT TOP 10 * FROM c"):
                        items.append(item)
                    content = f"Sample documents (first 10):\n{items}"
                else:
                    raise ValueError(f"Unknown resource URI: {uri_str}")
                
                return {"content": content}
            except Exception as e:
                return JSONResponse(content={"error": str(e)}, status_code=500)
        
        @self.app.get("/mcp/tools")
        async def list_tools():
            try:
                tools = [
                    {
                        "name": "query_documents",
                        "description": "Execute a SQL query against Cosmos DB documents",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {"type": "string", "description": "SQL query to execute"},
                                "parameters": {"type": "array", "items": {"type": "object"}, "description": "Optional query parameters"},
                                "cross_partition": {"type": "boolean", "description": "Enable cross-partition query", "default": True}
                            },
                            "required": ["query"]
                        }
                    },                    
                    {
                        "name": "read_document",
                        "description": "Read a specific document by ID",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "document_id": {"type": "string", "description": "Document ID to read"},
                                "partition_key": {"type": "string", "description": "Partition key value"}
                            },
                            "required": ["document_id", "partition_key"]
                        }
                    },
                    {
                        "name": "get_container_statistics",
                        "description": "Get statistics about the container",
                        "inputSchema": {"type": "object", "properties": {}, "required": []}
                    }
                ]
                return {"tools": tools}
            except Exception as e:
                return JSONResponse(content={"error": str(e)}, status_code=500)
        
        @self.app.post("/mcp/tools/{tool_name}")
        async def call_tool(tool_name: str, request: Request):
            try:
                body = await request.json()
                arguments = body.get("arguments", {})
                
                if not self.cosmos_client:
                    await self._initialize_cosmos_client()
                
                if tool_name == "query_documents":
                    result = await self._query_documents(arguments)
                elif tool_name == "read_document":
                    result = await self._read_document(arguments)
                elif tool_name == "get_container_statistics":
                    result = await self._get_container_statistics()
                else:
                    raise ValueError(f"Unknown tool: {tool_name}")
                
                return {"result": [{"type": "text", "text": str(result)}]}
                
            except Exception as e:
                error_msg = f"Error executing {tool_name}: {str(e)}"
                logger.error(error_msg)
                return JSONResponse(content={"error": error_msg}, status_code=500)
    
    async def run(self):
        """Run the HTTP MCP server"""
        # Validate configuration
        if not all([self.config.endpoint, self.config.key, self.config.database_name, self.config.container_name]):
            raise ValueError("Missing required Cosmos DB configuration. Please check your environment variables.")
        
        # Initialize Cosmos DB connection
        await self._initialize_cosmos_client()
        
        # Start the HTTP server
        config = uvicorn.Config(
            app=self.app,
            host=self.server_config.host,
            port=self.server_config.port,
            log_level=self.server_config.log_level,
            loop="asyncio"
        )
        server = uvicorn.Server(config)
        await server.serve()

async def main():
    """Main entry point"""
    server = CosmosDBMCPServer()
    await server.run()

if __name__ == "__main__":
    asyncio.run(main())
