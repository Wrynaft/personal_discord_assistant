import asyncio
from services.llm_service import LLMService
import config

async def test():
    print(f"Testing LLM Service with model: {config.GROQ_MODEL}")
    service = LLMService()
    messages = [{"role": "user", "content": "Say 'Hello World' if you can hear me."}]
    try:
        response = await service.generate_response(messages)
        print(f"Response: {response}")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    asyncio.run(test())
