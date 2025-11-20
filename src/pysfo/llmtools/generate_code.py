#%%

def generate_code_from_df(df, prompt, big_size = False):
    """
    Take a dataframe and a natural language prompt, 
    send them to an LLM, and return the analysis as text.
    """

    import pandas as pd
    from .config import get_client

    client = get_client()
    
    if not big_size and (df.memory_usage(deep=True).sum() > 3e6):
        raise ValueError("Dataset too big — will exceed $1 budget. Use big_size=True if intentional.")
    
    # Convert a sample of the data to CSV for context
    send = df.to_csv(index=False)

    # Build the system/user message
    messages = [
        {
            "role": "system", 
            "content": "\n".join([
                "You are a Python data analysis assistant. Follow these rules:",
                "1. Do not return anything, but the code that the user is asking for.",
                "2. Do not show the lines of code that generates the data for you. More exactly:",
                "\t 2.i.  If the user tells you what is the name of the dataframe, suppose you are starting from a dataset named just like that."
                "\t 2.ii. If the user does not tell you what is the name of the dataframe, suppose you are starting from a dataset named df."
            ])
        },
        {"role": "user", "content": f"Here is the dataset:\n{send}\n\nTask: {prompt}"}
    ]

    # Call the model
    response = client.chat.completions.create(
        model="gpt-4o-mini",
        messages=messages,
    )
    
    return response.choices[0].message.content
# %%
