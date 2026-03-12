

# ========== headers ========== #

echo "#--------------------------------------#"
echo "# STARTING PYSFO PROJECT ENV           #"
echo "#--------------------------------------#"

# ========== load master environment ========== #

source $HOME/.env_machine

# ========== activate project conda env ========== #

initialize_micromamba
micromamba activate pysfo

# ========== project level globals ========== #

# DO NOT USE SCRIPTDIR. THIS FILE WILL BE RUN FROM DIFFERENT FILES,
# SO PLEASE CALL GLOBALS ALREADY DEFINED IN MASTER ENVIRONMENT

export PROJECT_ROOT="$DEV_PATH/pysfo"
export LSEG_DATA_EMAIL="saf9215@stern.nyu.edu"

# ========== project level aliases ========== #d

# alias cd_documents='cd $DATA_RAW/factset/documents'
# alias cd_symbology='cd $DATA_TEMP/factset/symbology'
# alias cd_ownership='cd $DATA_RAW/factset/ownership'

# ========== work variables ========== #d

export_envfile "$PROJECT_ROOT/.vscode/.env" \
    PROJECT_ROOT \
    DATA_TEMP \
    LSEG_DATA_EMAIL \
    LSEG_DATA_API_KEY \
    LSEG_DATA_PASSWORD 

# ========== env activation assetion ========== #

echo "[pysfo] Environment pysfo activated succesfully"


