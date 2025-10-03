


def get_client():
    
    from openai import OpenAI
    import os

    api_key = os.environ.get("OPENAI_API_KEY")
    if not api_key:
        raise EnvironmentError("OPENAI_API_KEY environment variable is not set. Please set it before using OpenAI features.")
    return OpenAI(api_key = api_key)
