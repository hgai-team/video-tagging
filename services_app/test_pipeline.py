import asyncio
import logging
from datetime import datetime, timedelta
from unlabled_pipeline import PipelineProcessor

# Configure basic logging for testing
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

async def test_pipeline():
    """Test the full pipeline with sample parameters"""
    
    processor = PipelineProcessor(10,10)  
    
    # Test parameters
    media_type = 2
    end_date = "2025-06-02T00:00:00.000Z"
    start_date = "2015-05-31T00:00:00.000Z"
    collection_name = "test_collection"
    
    print(f"Testing pipeline with:")
    print(f"Media Type: {media_type}")
    print(f"Start Date: {start_date}")
    print(f"End Date: {end_date}")
    print(f"Collection: {collection_name}")
    print("-" * 50)
    
    try:
        await processor.process_daily_batch(
            media_type=media_type,
            start_date=start_date,
            end_date=end_date,
            collection_name=collection_name
        )
        print("✅ Pipeline test completed successfully!")
        
    except Exception as e:
        print(f"❌ Pipeline test failed: {e}")
        import traceback
        traceback.print_exc()

async def test_single_step():
    """Test individual pipeline steps"""
    
    print("Testing individual steps...")
    
    # Import the router functions for testing
    try:
        from api.router.stock_router import get_unlabel_resources
        
        # Test Step 1
        print("Testing get_unlabel_resources...")
        result = await get_unlabel_resources(
            media_type=1,
            start_date="2025-06-01",
            end_date="2025-06-02"
        )
        print(f"Step 1 result: {result}")
        
    except Exception as e:
        print(f"Step test failed: {e}")

if __name__ == "__main__":
    print("🚀 Starting Pipeline Tests")
    print("=" * 50)
    
    # Choose test mode
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "single":
        asyncio.run(test_single_step())
    else:
        asyncio.run(test_pipeline())