name: Deevv Search - PageRank

on:
  workflow_dispatch:

jobs:
  pagerank:
    runs-on: ubuntu-latest

    timeout-minutes: 60

    steps:
      - name: Checkout repository
        uses: actions/checkout@v4

      - name: Setup Python
        uses: actions/setup-python@v5
        with:
          python-version: "3.11"

      - name: Install dependencies
        run: |
          python -m pip install --upgrade pip
          pip install requests

      - name: Run PageRank
        env:
          CF_ACCOUNT_ID: ${{ secrets.CF_ACCOUNT_ID }}
          CF_D1_DATABASE_ID: ${{ secrets.CF_D1_DATABASE_ID }}
          CF_API_TOKEN: ${{ secrets.CF_API_TOKEN }}
        run: |
          python pagerank.py
