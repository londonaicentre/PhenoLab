#!/bin/bash

set -e

# Parse arguments
UPDATE_ALL=true
UPDATE_HDRUK=false
UPDATE_OPENCODELISTS=false
UPDATE_NHS_SNOMED=false

while [[ $# -gt 0 ]]; do
    case $1 in
        --hdruk)
            UPDATE_HDRUK=true
            UPDATE_ALL=false
            shift
            ;;
        --opencodelists)
            UPDATE_OPENCODELISTS=true
            UPDATE_ALL=false
            shift
            ;;
        --nhs-snomed)
            UPDATE_NHS_SNOMED=true
            UPDATE_ALL=false
            shift
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Run updates
if [ "$UPDATE_ALL" = true ] || [ "$UPDATE_HDRUK" = true ]; then
    echo "Updating HDR UK..."
    cd hdruk && python fetch_hdruk.py && cd ..
fi

if [ "$UPDATE_ALL" = true ] || [ "$UPDATE_OPENCODELISTS" = true ]; then
    echo "Updating OpenCodelists..."
    cd opencodelists && python fetch_opencodelists.py && cd ..
fi

if [ "$UPDATE_ALL" = true ] || [ "$UPDATE_NHS_SNOMED" = true ]; then
    echo "Updating NHS SNOMED..."
    cd ontoserver && python fetch_nhs_snomed.py && cd ..
fi

echo "Done"