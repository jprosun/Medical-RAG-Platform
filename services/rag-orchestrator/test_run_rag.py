import asyncio
import os
import sys

# Add path so imports work
sys.path.insert(0, os.path.abspath("."))
sys.path.insert(0, os.path.abspath(".."))

from app.main import ask
from app.schemas import RAGRequest

# Set required env vars for testing if not set
os.environ["QDRANT_URL"] = "http://localhost:6333"
os.environ["QDRANT_COLLECTION"] = "staging_medqa_vi_vmj_v2"
os.environ["EMBEDDING_MODEL"] = "BAAI/bge-m3"

async def test_rag():
    print("="*60)
    print("  Testing RAG Pipeline with Phase 1+2 Enhancements")
    print("="*60)
    
    # We choose a query that targets treatments or outcomes to trigger extraction 
    # and maybe conflict detection.
    query_text = "Phẫu thuật nội soi rạch bao xơ ở bệnh nhân ung thư vú có hiệu quả không?"
    
    print(f"QUERY: {query_text}\n")
    print("Running orchestrator...")
    
    req = RAGRequest(
        query=query_text,
        session_id="test_session_xyz"
    )
    
    try:
        response = await ask(req)
        print("\n\n" + "="*60)
        print("✅ SUCCESS")
        print("="*60)
        print(f"💡 ANSWER:\n{response.answer}\n")
        
        print("🔍 EVIDENCE METADATA:")
        print(f"  Query Type:      {response.metadata.get('query_type')}")
        print(f"  Coverage Level:  {response.metadata.get('coverage_level')}")
        print(f"  Conflicts Found: {len(response.metadata.get('conflict_notes', []))}")
        if response.metadata.get('conflict_notes'):
            for note in response.metadata['conflict_notes']:
                print(f"    - {note}")
                
        print(f"  Sources Used:    {len(response.sources)}")
        for src in response.sources:
            print(f"    - {src.title}")
            
    except Exception as e:
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_rag())
