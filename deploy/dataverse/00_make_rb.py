#!/usr/bin/env python3

import pandas as pd
import os
from pathlib import Path

def handle_metadata_block(df):
    return f"""
metadatablock.name={df.iloc[0, :]["name"]}
metadatablock.displayName={df.iloc[0, :]["displayName"]}
"""

def handle_dataset_field(df):
    lns = list()
    for e in df.itertuples():
        if e.metadatablock_id != 'LMTData':
            continue
        lns.append(
        f"""
datasetfieldtype.{e.name}.title={e.title}
datasetfieldtype.{e.name}.description={e.description}
datasetfieldtype.{e.name}.watermark={e.watermark}
"""
        )
    return ''.join(lns)


def handle_controlled_vocabulary(df):
    lns = list()
    for e in df.itertuples():
        key = e.Value.replace(" ", '_').lower()
        lns.append(
                f"""
controlledvocabulary.{e.DatasetField}.{key}={e.Value}
""")
    return ''.join(lns)


if __name__ == '__main__':
    import sys
    tsv_file = Path(sys.argv[1])

    data = pd.read_csv(tsv_file, sep='\t', header=None)
    # split tables
    sections = list()
    for i, v in enumerate(data.iloc[:, 0]):
        if isinstance(v, str) and v.startswith('#'):
            if sections and len(sections[-1]) < 3:
                # add end index for previous section
                sections[-1].append(i)
            sections.append([v, i])
    sections[-1].append(len(data))
    print(sections)

    def _make_section_df(v0, v1):
        d = data.iloc[v0 + 1:v1, :].copy()
        d = d.rename(columns={i: j for i, j in enumerate(data.iloc[v0, :])})
        print(d)
        return d

    sections = {
            k: _make_section_df(v0, v1)
            for k, v0, v1 in sections
            }
    print(sections)

    content = ''
    content += handle_metadata_block(sections['#metadataBlock'])
    content += handle_dataset_field(sections['#datasetField'])
    content += handle_controlled_vocabulary(sections['#controlledVocabulary'])
    content = "".join([s for s in content.splitlines(True) if s.strip("\r\n")])

    print(content)

    with open(tsv_file.with_suffix('.resource_bundle'), 'w') as fo:
        fo.write(content)
