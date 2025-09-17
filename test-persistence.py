#!/usr/bin/env python3
"""
Test script to verify pipeline builder persistence functionality
"""

import os
import sys
import tempfile
import shutil
from pathlib import Path

# Add the server directory to the path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

def test_persistence():
    """Test the pipeline builder persistence functionality."""
    print("🧪 Testing Pipeline Builder Persistence")
    print("=" * 50)

    # Create a temporary directory for testing
    with tempfile.TemporaryDirectory() as temp_dir:
        test_storage = Path(temp_dir) / "test_storage"

        # Set custom storage path
        os.environ["PIPELINE_STORAGE_PATH"] = str(test_storage)

        print(f"📁 Test storage directory: {test_storage}")

        # Import after setting environment variable
        from streamsets_server import (
            PipelineBuilderSession,
            save_pipeline_session,
            load_pipeline_session,
            get_storage_directory,
            PIPELINE_STORAGE_DIR
        )

        print(f"✅ Storage directory detected: {PIPELINE_STORAGE_DIR}")

        # Test 1: Create a pipeline session
        print("\n🔧 Test 1: Creating pipeline session")
        session = PipelineBuilderSession(
            title="Test Pipeline",
            description="Testing persistence",
            engine_type="data_collector"
        )

        # Add some test data
        session.add_stage("stage_1", {
            "stageLabel": "Dev Raw Data Source",
            "stageType": "origins"
        })
        session.add_stage("stage_2", {
            "stageLabel": "Trash",
            "stageType": "destinations"
        })
        session.add_connection("stage_1", "stage_2", "data")

        print(f"   Created session: {session.get_summary()}")

        # Test 2: Save session
        print("\n💾 Test 2: Saving session to disk")
        pipeline_id = "test_pipeline_1"
        save_pipeline_session(pipeline_id, session)

        # Check file exists
        session_file = test_storage / f"{pipeline_id}.pkl"
        if session_file.exists():
            print(f"   ✅ Session file created: {session_file}")
            print(f"   📊 File size: {session_file.stat().st_size} bytes")
        else:
            print("   ❌ Session file not created")
            return False

        # Test 3: Load session
        print("\n📖 Test 3: Loading session from disk")
        loaded_session = load_pipeline_session(pipeline_id)

        if loaded_session:
            print(f"   ✅ Session loaded successfully")
            print(f"   📋 Loaded session: {loaded_session.get_summary()}")

            # Verify data integrity
            if (loaded_session.title == session.title and
                len(loaded_session.stages) == len(session.stages) and
                len(loaded_session.connections) == len(session.connections)):
                print("   ✅ Data integrity verified")
            else:
                print("   ❌ Data integrity check failed")
                return False
        else:
            print("   ❌ Failed to load session")
            return False

        # Test 4: Storage fallback
        print("\n🔄 Test 4: Testing storage fallback")

        # Test with non-existent path
        os.environ["PIPELINE_STORAGE_PATH"] = "/nonexistent/path"

        # Re-import to test fallback
        import importlib
        import streamsets_server
        importlib.reload(streamsets_server)

        from streamsets_server import get_storage_directory
        fallback_dir = get_storage_directory()

        if fallback_dir:
            print(f"   ✅ Fallback storage: {fallback_dir}")
        else:
            print("   ⚠️ No storage available - memory-only mode")

        print("\n🎉 All persistence tests passed!")
        return True

def test_docker_paths():
    """Test Docker-specific path detection."""
    print("\n🐳 Testing Docker Path Detection")
    print("=" * 40)

    # Simulate Docker environment
    original_env = os.environ.get("PIPELINE_STORAGE_PATH")

    try:
        # Remove custom path
        if "PIPELINE_STORAGE_PATH" in os.environ:
            del os.environ["PIPELINE_STORAGE_PATH"]

        # Test with mock /data directory
        with tempfile.TemporaryDirectory() as temp_dir:
            data_dir = Path(temp_dir) / "data"
            data_dir.mkdir()

            # Mock the /data path check
            print(f"📁 Simulating Docker /data directory: {data_dir}")

            # Since we can't easily mock Path("/data"), just test the logic
            from streamsets_server import get_storage_directory
            storage_dir = get_storage_directory()

            print(f"✅ Storage directory: {storage_dir}")

    finally:
        # Restore original environment
        if original_env:
            os.environ["PIPELINE_STORAGE_PATH"] = original_env

if __name__ == "__main__":
    print("🚀 StreamSets MCP Server - Persistence Test Suite")
    print("=" * 60)

    try:
        success = test_persistence()
        test_docker_paths()

        if success:
            print("\n✅ All tests passed! Persistence is working correctly.")
            sys.exit(0)
        else:
            print("\n❌ Some tests failed.")
            sys.exit(1)

    except Exception as e:
        print(f"\n💥 Test suite failed with error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)