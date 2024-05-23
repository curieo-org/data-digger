import sys, os
dir_path = os.path.dirname(os.path.realpath(__file__))

sys.path.append(f"{dir_path}/..")

from utils.splade_embedding import SpladeEmbeddingsInference
from settings import SpladedocSettings

class SparseEmbeddingsTester:
    def __init__(self, settings):
        self.splade_model = SpladeEmbeddingsInference(
            model_name="",
            base_url=settings.spladedoc.api_url,
            auth_token=settings.spladedoc.api_key.get_secret_value(),
            timeout=60,
            embed_batch_size=settings.spladedoc.embed_batch_size)
    
    def compute_embedding(self, text:str):
        return self.splade_model.get_text_embedding_batch([text])
    

if __name__ == "__main__":
    splade_settings = SpladedocSettings()
    tester = SparseEmbeddingsTester(splade_settings)
    # https://pubmed.ncbi.nlm.nih.gov/38696263/
    alz1 = """Two of every three persons living with dementia reside in low- and middle-income countries (LMICs). The projected increase in global dementia rates is expected to affect LMICs disproportionately."""
    print(tester.compute_embedding(alz1))