# Frame-based extraction of the blending property

This repository contains a single script that reproduces the frame-based analysis reported in my PhD thesis.

## What the script does

1. Retrieves MetaNet metaphors and their frame mappings from Framester  
   It queries the Framester SPARQL endpoint for each selected metaphor IRI and extracts source and target frames, optional role mappings, entailment descriptions, and example sentences.

2. Expands frame typing using near-equivalences  
   For every frame encountered, it expands candidate equivalents using `skos:closeMatch` and `schema:subsumedUnder`, then checks whether each candidate occurs as a source or target frame in any metaphor. This mirrors the typing diagnostics used in our pipeline.

3. Computes surface commonalities between paired frames  
   For each source–target pair, it pulls frame element labels and linked WordNet synset labels and computes intersections. It reports both counts and lists, aligning with the semantic and structural similarity probes in the paper.

## Inputs

The list of metaphor IRIs is defined at the top of `blend_analysis_pipeline.py` in `SELECTED_METAPHORS`. 

The script queries the public Framester SPARQL endpoint by default.


## Outputs

The script produces three CSV files.

`metaphor_mappings_roles_entailments.csv`  
Columns: `metaphor`, `source_frame`, `target_frame`, `source_role`, `target_role`, `entailment`, `example`.

`frame_typing_expanded.csv`  
Columns: `seed_frame`, `equivalent_or_related_frames`, `as_source`, `as_target`.

`similarity_overlap.csv`  
Columns: `source_frame`, `target_frame`, `n_common_frame_elements`, `n_common_synset_labels`, `common_frame_elements`, `common_synset_labels`.

