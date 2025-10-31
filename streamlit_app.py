"""
Dashboard Footballistiques - Premier League 2024-2025
Application Streamlit simple avec connexion PostgreSQL
"""

import streamlit as st
import pandas as pd
import plotly.express as px
from sqlalchemy import create_engine, MetaData, Table, select, func, desc, case

# Configuration de la page
st.set_page_config(
    page_title="Dashboard Footballistiques",
    page_icon="⚽",
    layout="wide"
)

# ============================================
# 1. Connexion à la base de données
# ============================================

@st.cache_resource
def connecter_bdd():
    """Se connecte à la base de données PostgreSQL"""
    try:
        engine = create_engine("postgresql+psycopg2://postgres:malika123@localhost:5432/footballistiques")
        metadata = MetaData()
        
        # Charger les tables
        saison = Table("saison", metadata, autoload_with=engine)
        competition = Table("competition", metadata, autoload_with=engine)
        team = Table("team", metadata, autoload_with=engine)
        player = Table("player", metadata, autoload_with=engine)
        match = Table("match", metadata, autoload_with=engine)
        match_result = Table("match_result", metadata, autoload_with=engine)
        player_statistics = Table("player_statistics", metadata, autoload_with=engine)
        
        return engine, {
            "saison": saison,
            "competition": competition,
            "team": team,
            "player": player,
            "match": match,
            "match_result": match_result,
            "player_statistics": player_statistics
        }
    except Exception as e:
        st.error(f"Erreur de connexion à la base de données : {e}")
        return None, None

# Connexion
engine, tables = connecter_bdd()

if engine is None:
    st.stop()

# ============================================
# 2. Fonctions pour récupérer les données
# ============================================

def get_top_buteurs(nombre=10, equipe_filtre=None):
    """Récupère le top N des meilleurs buteurs"""
    stmt = select(
        tables["player"].c.Player,
        tables["team"].c.team_name,
        func.sum(tables["player_statistics"].c.Gls).label("total_buts")
    ).join(
        tables["player_statistics"], 
        tables["player_statistics"].c.player_id == tables["player"].c.player_id
    ).join(
        tables["team"], 
        tables["player"].c.team_id == tables["team"].c.team_id
    )
    
    if equipe_filtre:
        stmt = stmt.where(tables["team"].c.team_name == equipe_filtre)
    
    stmt = stmt.group_by(
        tables["player"].c.Player, 
        tables["team"].c.team_name
    ).order_by(
        desc("total_buts")
    ).limit(nombre)
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Joueur", "Équipe", "Buts"])
    return df

def get_joueurs_decisifs(nombre=10, equipe_filtre=None):
    """Récupère les joueurs les plus décisifs"""
    stmt = select(
        tables["player"].c.Player,
        tables["team"].c.team_name,
        (func.sum(tables["player_statistics"].c.Gls) + func.sum(tables["player_statistics"].c.Ast)).label("total_decisive")
    ).join(
        tables["player_statistics"], 
        tables["player"].c.player_id == tables["player_statistics"].c.player_id
    ).join(
        tables["team"], 
        tables["player"].c.team_id == tables["team"].c.team_id
    )
    
    if equipe_filtre:
        stmt = stmt.where(tables["team"].c.team_name == equipe_filtre)
    
    stmt = stmt.group_by(
        tables["player"].c.Player, 
        tables["team"].c.team_name
    ).order_by(
        desc("total_decisive")
    ).limit(nombre)
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Joueur", "Équipe", "Total"])
    return df

def get_buts_par_equipe():
    """Récupère le total de buts par équipe"""
    stmt = select(
        tables["team"].c.team_name,
        func.sum(tables["player_statistics"].c.Gls).label("total_buts")
    ).join(
        tables["player"], 
        tables["player"].c.team_id == tables["team"].c.team_id
    ).join(
        tables["player_statistics"], 
        tables["player_statistics"].c.player_id == tables["player"].c.player_id
    ).group_by(
        tables["team"].c.team_name
    ).order_by(
        desc("total_buts")
    )
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Équipe", "Buts"])
    return df

def get_classement():
    """Récupère le classement des équipes"""
    stmt = select(
        tables["team"].c.team_name,
        func.sum(
            case(
                (tables["match_result"].c.Result == 'W', 3),
                (tables["match_result"].c.Result == 'D', 1),
                else_=0
            )
        ).label("points")
    ).join(
        tables["match"], 
        tables["match"].c.team_id == tables["team"].c.team_id
    ).join(
        tables["match_result"], 
        tables["match_result"].c.match_id == tables["match"].c.match_id
    ).group_by(
        tables["team"].c.team_name
    ).order_by(
        desc("points")
    )
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Équipe", "Points"])
    return df

def get_attaque_defense():
    """Récupère moyenne buts marqués et encaissés"""
    stmt = select(
        tables["team"].c.team_name,
        func.avg(tables["match_result"].c.GF).label("moy_buts_marques"),
        func.avg(tables["match_result"].c.GA).label("moy_buts_encais")
    ).join(
        tables["match"], 
        tables["match"].c.team_id == tables["team"].c.team_id
    ).join(
        tables["match_result"], 
        tables["match_result"].c.match_id == tables["match"].c.match_id
    ).group_by(
        tables["team"].c.team_name
    )
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Équipe", "Buts Marqués", "Buts Encaissés"])
    return df

def get_meilleure_defense():
    """Récupère les équipes avec la meilleure défense"""
    stmt = select(
        tables["team"].c.team_name,
        func.sum(tables["match_result"].c.GA).label("buts_encaisses")
    ).join(
        tables["match"], 
        tables["match"].c.team_id == tables["team"].c.team_id
    ).join(
        tables["match_result"], 
        tables["match_result"].c.match_id == tables["match"].c.match_id
    ).group_by(
        tables["team"].c.team_name
    ).order_by(
        "buts_encaisses"
    )
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Équipe", "Buts Encaissés"])
    return df

def get_nationalites(equipe_filtre=None):
    """Récupère la répartition des nationalités"""
    stmt = select(
        tables["team"].c.team_name,
        tables["player"].c.Nation,
        func.count(tables["player"].c.player_id).label("nb_joueurs")
    ).join(
        tables["player"], 
        tables["player"].c.team_id == tables["team"].c.team_id
    )
    
    if equipe_filtre:
        stmt = stmt.where(tables["team"].c.team_name == equipe_filtre)
    
    stmt = stmt.group_by(
        tables["team"].c.team_name, 
        tables["player"].c.Nation
    ).order_by(
        tables["team"].c.team_name, 
        func.count(tables["player"].c.player_id).desc()
    )
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    df = pd.DataFrame(results, columns=["Équipe", "Nationalité", "Nombre"])
    return df

def get_liste_equipes():
    """Récupère la liste de toutes les équipes"""
    stmt = select(tables["team"].c.team_name).order_by(tables["team"].c.team_name)
    
    with engine.connect() as conn:
        results = conn.execute(stmt).fetchall()
    
    return [row[0] for row in results]

# ============================================
# 3. Fonctions pour les graphiques
# ============================================

def graphique_barres(df, x_col, y_col, titre, couleur_col=None):
    """Crée un graphique en barres simple"""
    if couleur_col:
        fig = px.bar(df, x=x_col, y=y_col, color=couleur_col, orientation='h', title=titre)
    else:
        fig = px.bar(df, x=x_col, y=y_col, orientation='h', title=titre)
    fig.update_layout(yaxis={'categoryorder': 'total ascending'}, height=400)
    return fig

def graphique_scatter(df, x_col, y_col, titre):
    """Crée un graphique scatter simple"""
    fig = px.scatter(df, x=x_col, y=y_col, size=x_col, hover_data=["Équipe"], title=titre)
    fig.update_layout(height=400)
    return fig

# ============================================
# 4. Interface principale
# ============================================

st.title("📊 Dashboard Footballistiques - Premier League 2024-2025")
st.markdown("---")

# Menu de navigation
page = st.sidebar.selectbox(
    "Choisir une section",
    ["Accueil", "Statistiques Joueurs", "Statistiques Équipes"]
)

# Liste des équipes pour les filtres
equipes_liste = get_liste_equipes()

# ============================================
# Page Accueil
# ============================================
if page == "Accueil":
    st.header("Bienvenue sur le Dashboard")
    
    # Récupérer quelques statistiques
    classement = get_classement()
    buts_equipe = get_buts_par_equipe()
    
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Équipes", len(equipes_liste))
    with col2:
        st.metric("Équipe en tête", classement.iloc[0]["Équipe"] if len(classement) > 0 else "N/A")
    with col3:
        st.metric("Points du leader", int(classement.iloc[0]["Points"]) if len(classement) > 0 else 0)
    with col4:
        st.metric("Total Buts", int(buts_equipe["Buts"].sum()))
    
    st.markdown("---")
    st.write("Utilisez le menu de gauche pour explorer les différentes sections.")

# ============================================
# Page Statistiques Joueurs
# ============================================
elif page == "Statistiques Joueurs":
    st.header("Statistiques des Joueurs")
    
    # Filtres
    st.subheader("Filtres")
    col1, col2 = st.columns(2)
    with col1:
        equipe_filtre = st.selectbox("Filtrer par équipe", ["Toutes"] + equipes_liste)
    with col2:
        nombre_joueurs = st.slider("Nombre de joueurs", 5, 20, 10)
    
    # Appliquer le filtre
    equipe_selectionnee = None if equipe_filtre == "Toutes" else equipe_filtre
    
    # Récupérer les données
    top_buteurs = get_top_buteurs(nombre_joueurs, equipe_selectionnee)
    joueurs_decisifs = get_joueurs_decisifs(nombre_joueurs, equipe_selectionnee)
    nationalites = get_nationalites(equipe_selectionnee)
    
    # Graphiques
    st.subheader("Graphiques")
    
    tab1, tab2, tab3 = st.tabs(["Meilleurs Buteurs", "Joueurs Décisifs", "Nationalités"])
    
    with tab1:
        fig1 = graphique_barres(top_buteurs, "Buts", "Joueur", "Top Meilleurs Buteurs", "Équipe")
        st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        fig2 = graphique_barres(joueurs_decisifs, "Total", "Joueur", "Joueurs Décisifs (Buts + Passes)", "Équipe")
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        # Agréger par nationalité
        nat_agg = nationalites.groupby("Nationalité")["Nombre"].sum().reset_index()
        nat_agg = nat_agg.sort_values("Nombre", ascending=False).head(15)
        fig3 = graphique_barres(nat_agg, "Nombre", "Nationalité", "Top 15 Nationalités")
        st.plotly_chart(fig3, use_container_width=True)
    
    # Tableau interactif
    st.subheader("Données Filtrées - Meilleurs Buteurs")
    st.dataframe(top_buteurs, use_container_width=True, height=300)
    
    # Bouton de téléchargement
    csv_data = top_buteurs.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Télécharger en CSV",
        data=csv_data,
        file_name=f"top_buteurs_{equipe_filtre.lower().replace(' ', '_')}.csv",
        mime="text/csv"
    )

# ============================================
# Page Statistiques Équipes
# ============================================
elif page == "Statistiques Équipes":
    st.header("Statistiques des Équipes")
    
    # Récupérer les données
    buts_equipe = get_buts_par_equipe()
    classement = get_classement()
    attaque_defense = get_attaque_defense()
    defense = get_meilleure_defense()
    
    # Graphiques
    st.subheader("Graphiques")
    
    tab1, tab2, tab3, tab4 = st.tabs(["Classement", "Buts par Équipe", "Attaque vs Défense", "Meilleure Défense"])
    
    with tab1:
        fig1 = graphique_barres(classement, "Points", "Équipe", "Classement des Équipes")
        st.plotly_chart(fig1, use_container_width=True)
    
    with tab2:
        fig2 = graphique_barres(buts_equipe, "Buts", "Équipe", "Total de Buts par Équipe")
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab3:
        fig3 = graphique_scatter(attaque_defense, "Buts Marqués", "Buts Encaissés", "Attaque vs Défense")
        st.plotly_chart(fig3, use_container_width=True)
    
    with tab4:
        fig4 = graphique_barres(defense, "Buts Encaissés", "Équipe", "Meilleure Défense (moins de buts encaissés)")
        st.plotly_chart(fig4, use_container_width=True)
    
    # Tableau interactif - Classement
    st.subheader("Données Filtrées - Classement")
    
    # Fusionner les données pour un tableau complet
    tableau_complet = classement.merge(buts_equipe, on="Équipe", how="left")
    tableau_complet = tableau_complet.merge(
        attaque_defense[["Équipe", "Buts Marqués", "Buts Encaissés"]], 
        on="Équipe", 
        how="left"
    )
    
    st.dataframe(tableau_complet, use_container_width=True, height=400)
    
    # Bouton de téléchargement
    csv_data = tableau_complet.to_csv(index=False).encode('utf-8')
    st.download_button(
        label="Télécharger en CSV",
        data=csv_data,
        file_name="statistiques_equipes.csv",
        mime="text/csv"
    )
