from openai import AsyncOpenAI
import config

class LLMService:
    def __init__(self):
        self.client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=config.OPENROUTER_API_KEY,
        )
        self.model = config.OPENROUTER_MODEL

    async def generate_response(self, messages):
        """
        Generates a response from the LLM based on the message history.
        Args:
            messages (list): A list of message dictionaries (role, content).
        Returns:
            str: The generated response content.
        """
        try:
            completion = await self.client.chat.completions.create(
                model=self.model,
                messages=messages
            )
            return completion.choices[0].message.content
        except Exception as e:
            import traceback
            traceback.print_exc()
            print(f"Error generating response: {e}")
            return "Sorry, I encountered an error while processing your request."
