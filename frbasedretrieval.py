#Script that:
# 1) fetches MetaNet metaphor -> source/target frames (+ roles, entailments, examples)
# 2) expands frame typing with closeMatch and subsumedUnder
# 3) computes common frame elements and WordNet synset label overlap per source–target pair
#
# Dependencies: pip install SPARQLWrapper pandas tqdm

from SPARQLWrapper import SPARQLWrapper, JSON
import pandas as pd
from tqdm import tqdm
import time

FRAMESTER_SPARQL = "https://etna.istc.cnr.it/framester2/sparql"

SELECTED_METAPHORS = [
    "https://w3id.org/framester/metanet/metaphors/GOVERNMENT_INSTITUTION_IS_A_BUILDING",
    "https://w3id.org/framester/metanet/metaphors/GOVERNMENT_IS_A_PERSON",
    "https://w3id.org/framester/metanet/metaphors/GOVERNMENT_IS_AN_ORGANISM",
    "https://w3id.org/framester/metanet/metaphors/GOVERNING_ACTION_IS_MOTION",
    "https://w3id.org/framester/metanet/metaphors/GOVERNMENT_INSTITUTION_IS_A_PHYSICAL_STRUCTURE",
    "https://w3id.org/framester/metanet/metaphors/CAUSED_CHANGE_OF_STATE_IS_CAUSED_CHANGE_OF_LOCATION",
    "https://w3id.org/framester/metanet/metaphors/ANALYZING_IS_DISSECTING",
    "https://w3id.org/framester/metanet/metaphors/MACHINES_ARE_PEOPLE",
    "https://w3id.org/framester/metanet/metaphors/CHANGE_IN_CONTROLLER_IS_TRANSFER_OF_POSSESSION",
    "https://w3id.org/framester/metanet/metaphors/DISEASE_TREATMENT_IS_WAR"
]

def run_sparql(query, endpoint=FRAMESTER_SPARQL, retries=3, sleep_sec=0.8):
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    sparql.setQuery(query)
    for attempt in range(retries):
        try:
            return sparql.query().convert()
        except Exception as e:
            if attempt == retries - 1:
                raise
            time.sleep(sleep_sec * (attempt + 1))

def get_mappings_roles_entailments(metaphor_iri):
    q = f"""
    PREFIX metanet: <https://w3id.org/framester/metanet/schema/>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    SELECT DISTINCT ?metaphor ?src ?tgt ?srcRole ?tgtRole ?ent ?ex
    WHERE {{
      BIND(<{metaphor_iri}> AS ?metaphor)
      OPTIONAL {{ ?metaphor metanet:hasSourceFrame ?src. }}
      OPTIONAL {{ ?metaphor metanet:hasTargetFrame ?tgt. }}
      OPTIONAL {{ ?metaphor metanet:sourceRole ?srcRole. }}
      OPTIONAL {{ ?metaphor metanet:targetRole ?tgtRole. }}
      OPTIONAL {{ ?metaphor metanet:hasEntailmentDescription ?ent. }}
      OPTIONAL {{ ?metaphor metanet:hasExample ?ex. }}
    }}
    """
    data = run_sparql(q)
    rows = []
    for b in data["results"]["bindings"]:
        rows.append({
            "metaphor": b.get("metaphor", {}).get("value"),
            "source_frame": b.get("src", {}).get("value"),
            "target_frame": b.get("tgt", {}).get("value"),
            "source_role": b.get("srcRole", {}).get("value"),
            "target_role": b.get("tgtRole", {}).get("value"),
            "entailment": b.get("ent", {}).get("value"),
            "example": b.get("ex", {}).get("value")
        })
    return rows

def expand_equivalents_and_typing(frame_uri):
    q = f"""
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX schema: <http://schema.org/>
    PREFIX metanet: <https://w3id.org/framester/metanet/schema/>

    SELECT DISTINCT ?candidate ?typing
    WHERE {{
      {{
        BIND(<{frame_uri}> AS ?seed)
        VALUES ?p {{ skos:closeMatch schema:subsumedUnder }}
        {{ ?seed ?p ?candidate. }} UNION {{ ?candidate ?p ?seed. }}
      }}
      UNION
      {{
        BIND(<{frame_uri}> AS ?candidate)
      }}

      BIND("none" AS ?typing)
      BIND(COALESCE(?candidate, <{frame_uri}>) AS ?cand)

      OPTIONAL {{
        FILTER(BOUND(?cand))
        ?m metanet:hasSourceFrame ?cand.
        BIND("source" AS ?typing)
      }}
      OPTIONAL {{
        FILTER(BOUND(?cand))
        ?m2 metanet:hasTargetFrame ?cand.
        BIND("target" AS ?typing2)
      }}
    }}
    """
    data = run_sparql(q)
    src = set()
    tgt = set()
    cand_all = set()
    for b in data["results"]["bindings"]:
        c = b.get("candidate", {}).get("value", frame_uri)
        cand_all.add(c)
        t = b.get("typing", {}).get("value")
        # typing2 may appear only in raw bindings, not as named var; we re-check below
    # Second pass to type candidates robustly
    for c in list(cand_all):
        q2 = f"""
        PREFIX metanet: <https://w3id.org/framester/metanet/schema/>
        ASK {{ {{ ?m metanet:hasSourceFrame <{c}> }} UNION {{ <{c}> metanet:hasSourceFrame ?x }} }}
        """
        is_src = run_sparql(q2)["boolean"]
        q3 = f"""
        PREFIX metanet: <https://w3id.org/framester/metanet/schema/>
        ASK {{ {{ ?m metanet:hasTargetFrame <{c}> }} UNION {{ <{c}> metanet:hasTargetFrame ?x }} }}
        """
        is_tgt = run_sparql(q3)["boolean"]
        if is_src:
            src.add(c)
        if is_tgt:
            tgt.add(c)
    return {
        "seed": frame_uri,
        "candidates": list(cand_all),
        "as_source": list(src),
        "as_target": list(tgt)
    }

def get_frame_elements_and_synsets(frame_uri):
    q = f"""
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
    PREFIX skos: <http://www.w3.org/2004/02/skos/core#>
    PREFIX schema: <http://schema.org/>
    PREFIX metanet: <https://w3id.org/framester/metanet/schema/>
    PREFIX framenet: <https://w3id.org/framester/framenet/schema/>
    PREFIX wn: <https://w3id.org/framester/wn/wn30/schema/>

    SELECT DISTINCT ?feLabel ?synLabel
    WHERE {{
      BIND(<{frame_uri}> AS ?f)

      # Frame elements through common patterns
      OPTIONAL {{
        ?f ?feRel ?fe .
        VALUES ?feRel {{
          framenet:fe framenet:frameElement
          schema:hasPart
          metanet:sourceRole metanet:targetRole
        }}
        OPTIONAL {{ ?fe rdfs:label ?feLabel. }}
        OPTIONAL {{ ?fe skos:prefLabel ?feLabel. }}
      }}

      # Synset labels linked from frame or its lexical units
      OPTIONAL {{
        {{ ?f skos:closeMatch ?syn. }} UNION {{ ?f schema:sameAs ?syn. }} UNION {{ ?f framenet:lu ?lu. ?lu skos:closeMatch ?syn. }}
        FILTER(CONTAINS(STR(?syn), "wn"))
        OPTIONAL {{ ?syn rdfs:label ?synLabel. }}
        OPTIONAL {{ ?syn skos:prefLabel ?synLabel. }}
      }}
    }}
    """
    data = run_sparql(q)
    fe = set()
    syn = set()
    for b in data["results"]["bindings"]:
        fe_lab = b.get("feLabel", {}).get("value")
        syn_lab = b.get("synLabel", {}).get("value")
        if fe_lab:
            fe.add(fe_lab.strip())
        if syn_lab:
            syn.add(syn_lab.strip())
    return sorted(fe), sorted(syn)

def compute_overlap(source_frame, target_frame):
    fe_src, syn_src = get_frame_elements_and_synsets(source_frame)
    fe_tgt, syn_tgt = get_frame_elements_and_synsets(target_frame)
    common_fe = sorted(set(fe_src) & set(fe_tgt))
    common_syn = sorted(set(syn_src) & set(syn_tgt))
    return {
        "source_frame": source_frame,
        "target_frame": target_frame,
        "n_common_frame_elements": len(common_fe),
        "n_common_synset_labels": len(common_syn),
        "common_frame_elements": "; ".join(common_fe),
        "common_synset_labels": "; ".join(common_syn)
    }

def main():
    all_map_rows = []
    for m in tqdm(SELECTED_METAPHORS, desc="Fetching mappings and roles"):
        rows = get_mappings_roles_entailments(m)
        if not rows:
            all_map_rows.append({
                "metaphor": m, "source_frame": None, "target_frame": None,
                "source_role": None, "target_role": None, "entailment": None, "example": None
            })
        else:
            all_map_rows.extend(rows)

    df_map = pd.DataFrame(all_map_rows).drop_duplicates()
    df_map.to_csv("metaphor_mappings_roles_entailments.csv", index=False)

    # Frame typing expansion for every distinct frame we saw
    unique_frames = sorted(set(df_map["source_frame"].dropna().tolist() + df_map["target_frame"].dropna().tolist()))
    typing_rows = []
    for f in tqdm(unique_frames, desc="Expanding frame typing"):
        info = expand_equivalents_and_typing(f)
        typing_rows.append({
            "seed_frame": info["seed"],
            "equivalent_or_related_frames": "; ".join(sorted(info["candidates"])),
            "as_source": "; ".join(sorted(info["as_source"])),
            "as_target": "; ".join(sorted(info["as_target"]))
        })
    df_type = pd.DataFrame(typing_rows)
    df_type.to_csv("frame_typing_expanded.csv", index=False)

    overlap_rows = []
    for _, r in tqdm(df_map.dropna(subset=["source_frame", "target_frame"]).iterrows(), total=len(df_map.dropna(subset=["source_frame", "target_frame"])), desc="Computing overlaps"):
        overlap_rows.append(compute_overlap(r["source_frame"], r["target_frame"]))
    df_overlap = pd.DataFrame(overlap_rows).drop_duplicates()
    df_overlap.to_csv("similarity_overlap.csv", index=False)

    print("\nSummary")
    print(f"Metaphors processed: {len(SELECTED_METAPHORS)}")
    print(f"Pairs with frames: {df_map.dropna(subset=['source_frame','target_frame']).shape[0]}")
    if not df_overlap.empty:
        top = df_overlap.sort_values(["n_common_frame_elements","n_common_synset_labels"], ascending=False).head(5)
        print("\nTop pairs by surface overlap:")
        for _, row in top.iterrows():
            print(f"- {row['source_frame']} vs {row['target_frame']}  "
                  f"FE overlap={row['n_common_frame_elements']}  SYN overlap={row['n_common_synset_labels']}")

if __name__ == "__main__":
    main()
