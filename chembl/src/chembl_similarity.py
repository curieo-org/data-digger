from FPSim2 import FPSim2Engine


class ChemblSimilarity:
    def __init__(self):
        fpsim_file_path = 'chembl.h5'
        self.fpsim2 = FPSim2Engine(fpsim_file_path, in_memory_fps=False)

    def compound_similarity(self, canonical_smiles, threshold = 0.85) -> list[dict[str, str]]:
        result = self.fpsim2.on_disk_similarity(canonical_smiles, threshold, n_workers=10)

        data_dictionary = []

        for row in result:
            row = list(row)
            for index, value in enumerate(row):
                row[index] = str(value) if value is not None else ''

            molregno, similarity = row

            data_dictionary.append({
                'molregno': molregno,
                'similarity': similarity
            })
            
        return data_dictionary