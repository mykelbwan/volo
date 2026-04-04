import threading
from unittest.mock import MagicMock, patch
import pytest
from llms import llms_init

@pytest.fixture(autouse=True)
def clear_cache():
    """Clear the model cache before each test."""
    with llms_init._CACHE_LOCK:
        llms_init._MODEL_CACHE.clear()

def test_lazy_loading_conversation_llm():
    with patch("llms.llms_init.ChatGoogleGenerativeAI") as mock_genai:
        # Accessing the attribute for the first time should trigger the builder
        model = llms_init.conversation_llm
        assert mock_genai.called
        assert model == mock_genai.return_value

def test_caching_behavior():
    with patch("llms.llms_init.ChatGoogleGenerativeAI") as mock_genai:
        # First access
        model1 = llms_init.conversation_llm
        # Second access
        model2 = llms_init.conversation_llm
        
        assert mock_genai.call_count == 1
        assert model1 is model2

def test_interaction_layer_initialization():
    with patch("llms.llms_init.ChatGoogleGenerativeAI") as mock_genai:
        model = llms_init.interaction_layer
        assert mock_genai.called
        # Check if temperature 0 was passed
        args, kwargs = mock_genai.call_args
        assert kwargs["temperature"] == 0

def test_planning_llm_initialization():
    with patch("llms.llms_init.ChatCohere") as mock_cohere:
        model = llms_init.planning_llm
        assert mock_cohere.called
        assert model == mock_cohere.return_value

def test_invalid_attribute_raises_error():
    with pytest.raises(AttributeError):
        _ = llms_init.non_existent_model

def test_thread_safety():
    """Verify that multiple threads accessing the same LLM only trigger one build."""
    call_count = 0
    def mock_builder():
        nonlocal call_count
        call_count += 1
        return MagicMock()

    with patch.dict(llms_init._BUILDERS, {"test_model": mock_builder}):
        threads = []
        for _ in range(10):
            t = threading.Thread(target=lambda: getattr(llms_init, "test_model"))
            threads.append(t)
            t.start()
        
        for t in threads:
            t.join()
            
        assert call_count == 1

def test_json_parser_initialization():
    with patch("llms.llms_init.HuggingFaceEndpoint") as mock_endpoint:
        with patch("llms.llms_init.ChatHuggingFace") as mock_chat_hf:
            # We need to ensure _get_or_build("json_parser_llm") works for _build_json_parser
            model = llms_init.json_parser
            assert mock_endpoint.called
            assert mock_chat_hf.called
            assert model == mock_chat_hf.return_value
