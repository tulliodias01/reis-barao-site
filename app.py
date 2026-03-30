import os
import json
import re
import subprocess
import sys
from datetime import datetime
from functools import wraps
from decimal import Decimal
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from flask import (
    Flask,
    abort,
    jsonify,
    redirect,
    render_template,
    request,
    session,
    flash,
    url_for,
)
from flask_sqlalchemy import SQLAlchemy
from sqlalchemy import func, or_
from sqlalchemy.orm import joinedload
from werkzeug.security import check_password_hash, generate_password_hash
from werkzeug.utils import secure_filename


BASE_DIR = os.path.abspath(os.path.dirname(__file__))

app = Flask(__name__)
app.config["SECRET_KEY"] = os.getenv("SECRET_KEY", "troque-esta-chave-em-producao")
app.config["SQLALCHEMY_DATABASE_URI"] = os.getenv(
    "DATABASE_URL", f"sqlite:///{os.path.join(BASE_DIR, 'imoveis.db')}"
)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False
app.config["UPLOAD_FOLDER"] = os.path.join(BASE_DIR, "static", "uploads")
app.config["MAX_CONTENT_LENGTH"] = 8 * 1024 * 1024
app.config["EXTRATOR_ORIGEM_PADRAO"] = os.getenv(
    "EXTRATOR_ORIGEM_PADRAO",
    r"D:\BACKUP IMOBILIARIA\PDF EXTRATOR\ACREDITE SONHOS  IMOVEIS ORGANIZADO\IMOVEIS",
)

db = SQLAlchemy(app)
_schema_checked = False

WORKING_LOCALIDADES = {
    "Salvador": [
        "Stella Maris",
        "Pituba",
        "Caminho das Arvores",
        "Itaigara",
        "Horto Florestal",
        "Ondina",
        "Barra",
        "Vitoria",
        "Rio Vermelho",
        "Brotas",
        "Patamares",
        "Pituacu",
        "Alphaville I",
        "Alphaville II",
        "Praia do Flamengo",
        "Itapua",
        "Costa Azul",
        "Armacao",
        "Imbui",
        "Jaguaribe",
    ],
    "Rio de Janeiro": [
        "Barra da Tijuca",
        "Recreio dos Bandeirantes",
        "Leblon",
        "Ipanema",
        "Copacabana",
        "Lagoa",
        "Jardim Botanico",
        "Gavea",
        "Sao Conrado",
        "Botafogo",
        "Flamengo",
        "Laranjeiras",
        "Urca",
        "Tijuca",
        "Humaita",
        "Joa",
        "Vargem Grande",
        "Vargem Pequena",
        "Freguesia",
        "Itanhanga",
    ],
}


def normalize_lookup(value):
    return slugify((value or "").strip())


imovel_categoria = db.Table(
    "imovel_categoria",
    db.Column("imovel_id", db.Integer, db.ForeignKey("imovel.id"), primary_key=True),
    db.Column("categoria_id", db.Integer, db.ForeignKey("categoria.id"), primary_key=True),
)


class Categoria(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    nome = db.Column(db.String(40), unique=True, nullable=False, index=True)
    slug = db.Column(db.String(60), unique=True, nullable=False, index=True)
    ativa = db.Column(db.Boolean, default=True, nullable=False)

    def __repr__(self):
        return f"<Categoria {self.nome}>"


class Imovel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    slug = db.Column(db.String(160), unique=True, nullable=False, index=True)
    codigo_origem = db.Column(db.String(120), index=True)
    titulo = db.Column(db.String(180), nullable=False, index=True)
    descricao = db.Column(db.Text, nullable=False)
    info_interna = db.Column(db.Text)
    cidade = db.Column(db.String(120), nullable=False, index=True)
    bairro = db.Column(db.String(120), nullable=False, index=True)
    cep = db.Column(db.String(9), index=True)
    endereco = db.Column(db.String(220))
    latitude = db.Column(db.Float)
    longitude = db.Column(db.Float)
    preco = db.Column(db.Numeric(14, 2), nullable=False, index=True)
    iptu = db.Column(db.Numeric(12, 2), default=0)
    condominio = db.Column(db.Numeric(12, 2), default=0)
    area_privativa = db.Column(db.Float, nullable=False)
    area_total = db.Column(db.Float)
    quartos = db.Column(db.Integer, default=0)
    suites = db.Column(db.Integer, default=0)
    banheiros = db.Column(db.Integer, default=0)
    vagas = db.Column(db.Integer, default=0)
    destaque = db.Column(db.Boolean, default=False, nullable=False)
    novo = db.Column(db.Boolean, default=False, nullable=False)
    ativo = db.Column(db.Boolean, default=True, nullable=False, index=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False, index=True)
    updated_at = db.Column(
        db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False
    )

    categorias = db.relationship(
        "Categoria", secondary=imovel_categoria, lazy="subquery", backref="imoveis"
    )
    imagens = db.relationship(
        "ImagemImovel",
        backref="imovel",
        lazy="select",
        cascade="all, delete-orphan",
        order_by="ImagemImovel.ordem.asc()",
    )

    @property
    def capa(self):
        return next((img for img in self.imagens if img.eh_capa), self.imagens[0] if self.imagens else None)

    @property
    def url_publica(self):
        return url_for("detalhe_imovel", slug=self.slug, imovel_id=self.id)

    def to_card_json(self):
        return {
            "id": self.id,
            "titulo": self.titulo,
            "cidade": self.cidade,
            "bairro": self.bairro,
            "preco": float(self.preco),
            "quartos": self.quartos,
            "banheiros": self.banheiros,
            "vagas": self.vagas,
            "suites": self.suites,
            "area_privativa": self.area_privativa,
            "destaque": self.destaque,
            "novo": self.novo,
            "url": self.url_publica,
            "img": self.capa.url_800 if self.capa else url_for("static", filename="img/fallback-imovel.jpg"),
            "categorias": [c.nome for c in self.categorias],
        }

    def __repr__(self):
        return f"<Imovel {self.titulo}>"


class ImagemImovel(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    imovel_id = db.Column(db.Integer, db.ForeignKey("imovel.id"), nullable=False, index=True)
    legenda = db.Column(db.String(160))
    ordem = db.Column(db.Integer, default=0, nullable=False)
    eh_capa = db.Column(db.Boolean, default=False, nullable=False)
    url_400 = db.Column(db.String(255), nullable=False)
    url_800 = db.Column(db.String(255), nullable=False)
    url_1600 = db.Column(db.String(255), nullable=False)
    webp_800 = db.Column(db.String(255))
    webp_1600 = db.Column(db.String(255))
    created_at = db.Column(db.DateTime, default=datetime.utcnow, nullable=False)

    def __repr__(self):
        return f"<ImagemImovel {self.id} do imovel {self.imovel_id}>"


def slugify(value):
    import re
    import unicodedata

    value = unicodedata.normalize("NFKD", value).encode("ascii", "ignore").decode("ascii")
    value = re.sub(r"[^\w\s-]", "", value.lower())
    return re.sub(r"[-\s]+", "-", value).strip("-")


def br_currency(value):
    if value is None:
        return "R$ 0,00"
    number = Decimal(value)
    formatted = f"{number:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    return f"R$ {formatted}"


def parse_brl_number(value, default=0.0):
    if value is None:
        return default
    if isinstance(value, (int, float, Decimal)):
        return float(value)

    text = str(value).strip()
    if not text:
        return default

    cleaned = re.sub(r"[^\d,.\-]", "", text)
    if not cleaned:
        return default

    # Caso comum BR: 1.234.567,89
    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    # Caso com milhares: 300.000
    elif "." in cleaned and cleaned.count(".") >= 1 and "," not in cleaned:
        parts = cleaned.split(".")
        if all(p.isdigit() for p in parts) and len(parts[-1]) == 3:
            cleaned = "".join(parts)
    # Decimal com virgula
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")

    try:
        return float(cleaned)
    except ValueError:
        return default


def _norm_place(value):
    return slugify((value or "").strip())


def normalize_cidade_bairro(cidade, bairro):
    cidade_raw = (cidade or "").strip()
    bairro_raw = (bairro or "").strip()
    c = _norm_place(cidade_raw)
    b = _norm_place(bairro_raw)

    bairro_to_cidade = {
        "stella-maris": "Salvador",
        "brotas": "Salvador",
        "praia-do-flamengo": "Salvador",
        "alphaville-i-salvador": "Salvador",
        "itacimirim": "Camacari",
    }

    if c in bairro_to_cidade:
        if not bairro_raw or b == c:
            bairro_raw = cidade_raw
        cidade_raw = bairro_to_cidade[c]
    elif c == "imoveis" and b in bairro_to_cidade:
        cidade_raw = bairro_to_cidade[b]
    elif c in {"camacari", "camacari-ba", "camacari-bahia", "camacari"}:
        cidade_raw = "Camacari"

    return cidade_raw.title(), bairro_raw.title()


def admin_required(view_func):
    @wraps(view_func)
    def wrapper(*args, **kwargs):
        if not session.get("admin_logged"):
            return redirect(url_for("admin_login", next=request.path))
        return view_func(*args, **kwargs)

    return wrapper


@app.context_processor
def inject_globals():
    return {"current_year": datetime.now().year}


@app.template_filter("brl")
def brl_filter(value):
    return br_currency(value)


def ensure_optional_columns():
    # Compatibilidade para bancos SQLite antigos sem migrations.
    inspector = db.inspect(db.engine)
    cols = {c["name"] for c in inspector.get_columns("imovel")}
    if "info_interna" not in cols:
        db.session.execute(db.text("ALTER TABLE imovel ADD COLUMN info_interna TEXT"))
        db.session.commit()
    if "codigo_origem" not in cols:
        db.session.execute(db.text("ALTER TABLE imovel ADD COLUMN codigo_origem VARCHAR(120)"))
        db.session.commit()
    if "cep" not in cols:
        db.session.execute(db.text("ALTER TABLE imovel ADD COLUMN cep VARCHAR(9)"))
        db.session.commit()


@app.before_request
def ensure_schema_on_request():
    global _schema_checked
    if _schema_checked:
        return
    try:
        ensure_optional_columns()
    except Exception:
        # Evita derrubar request em ambientes onde a tabela ainda nao exista.
        pass
    _schema_checked = True


def save_uploaded_image(file_storage):
    filename = secure_filename(file_storage.filename or "")
    if not filename:
        return None

    os.makedirs(app.config["UPLOAD_FOLDER"], exist_ok=True)
    base_name, ext = os.path.splitext(filename)
    candidate = filename
    counter = 2
    while os.path.exists(os.path.join(app.config["UPLOAD_FOLDER"], candidate)):
        candidate = f"{base_name}_{counter}{ext}"
        counter += 1

    full_path = os.path.join(app.config["UPLOAD_FOLDER"], candidate)
    file_storage.save(full_path)
    return url_for("static", filename=f"uploads/{candidate}")


def cleanup_image_file(static_url):
    if not static_url or not static_url.startswith("/static/uploads/"):
        return
    relative = static_url.replace("/static/", "", 1).replace("/", os.sep)
    file_path = os.path.join(app.static_folder, relative)
    if os.path.exists(file_path):
        try:
            os.remove(file_path)
        except OSError:
            pass


def build_imovel_query(params):
    query = Imovel.query.options(joinedload(Imovel.imagens), joinedload(Imovel.categorias)).filter(Imovel.ativo.is_(True))

    tipo = params.get("tipo", "").strip().lower()
    cidade = params.get("cidade", "").strip()
    bairro = params.get("bairro", "").strip()
    quartos_min = params.get("quartos_min", type=int)
    suites_min = params.get("suites_min", type=int)
    vagas_min = params.get("vagas_min", type=int)
    preco_min = parse_brl_number(params.get("preco_min"), None)
    preco_max = parse_brl_number(params.get("preco_max"), None)

    if tipo:
        query = query.join(Imovel.categorias).filter(Categoria.slug == slugify(tipo))
    if cidade:
        query = query.filter(Imovel.cidade.ilike(f"%{cidade}%"))
    if bairro:
        query = query.filter(Imovel.bairro.ilike(f"%{bairro}%"))
    if quartos_min is not None:
        query = query.filter(Imovel.quartos >= quartos_min)
    if suites_min is not None:
        query = query.filter(Imovel.suites >= suites_min)
    if vagas_min is not None:
        query = query.filter(Imovel.vagas >= vagas_min)
    if preco_min is not None:
        query = query.filter(Imovel.preco >= preco_min)
    if preco_max is not None:
        query = query.filter(Imovel.preco <= preco_max)

    sort = params.get("ordenar", "recentes")
    if sort == "preco_asc":
        query = query.order_by(Imovel.preco.asc(), Imovel.created_at.desc())
    elif sort == "preco_desc":
        query = query.order_by(Imovel.preco.desc(), Imovel.created_at.desc())
    else:
        query = query.order_by(Imovel.created_at.desc())

    return query


@app.route("/")
def index():
    query = build_imovel_query(request.args)
    imoveis = query.limit(12).all()
    cidade_filtrada = (request.args.get("cidade") or "").strip()
    hero_fallbacks = [item.capa.url_1600 for item in imoveis if item.capa]
    hero_rio_items = (
        Imovel.query.options(joinedload(Imovel.imagens))
        .filter(Imovel.ativo.is_(True), Imovel.cidade.ilike("%Rio de Janeiro%"))
        .order_by(Imovel.created_at.desc())
        .limit(6)
        .all()
    )
    hero_salvador_items = (
        Imovel.query.options(joinedload(Imovel.imagens))
        .filter(Imovel.ativo.is_(True), Imovel.cidade.ilike("%Salvador%"))
        .order_by(Imovel.created_at.desc())
        .limit(6)
        .all()
    )
    hero_rio_images = [item.capa.url_1600 for item in hero_rio_items if item.capa]
    hero_salvador_images = [item.capa.url_1600 for item in hero_salvador_items if item.capa]
    if not hero_rio_images and hero_fallbacks:
        hero_rio_images = [hero_fallbacks[0]]
    if not hero_salvador_images:
        hero_salvador_images = [hero_fallbacks[1]] if len(hero_fallbacks) > 1 else hero_rio_images[:1]
    categorias = Categoria.query.filter_by(ativa=True).order_by(Categoria.nome.asc()).all()
    cidades = [
        r[0]
        for r in db.session.query(Imovel.cidade)
        .filter(Imovel.ativo.is_(True), Imovel.cidade.isnot(None))
        .distinct()
        .order_by(Imovel.cidade.asc())
        .all()
        if r[0]
    ]
    bairros = [
        r[0]
        for r in db.session.query(Imovel.bairro)
        .filter(Imovel.ativo.is_(True), Imovel.bairro.isnot(None))
        .distinct()
        .order_by(Imovel.bairro.asc())
        .all()
        if r[0]
    ]

    meta = {
        "title": "REIS BARAO | Imoveis em Salvador e Rio de Janeiro",
        "description": "Imoveis para venda e locacao em Salvador e Rio de Janeiro com busca inteligente e atendimento direto.",
        "canonical": url_for("index", _external=True),
    }
    hero_cover = url_for("static", filename="img/riossa.png")
    return render_template(
        "index.html",
        imoveis=imoveis,
        cidade_filtrada=cidade_filtrada,
        hero_rio_images=hero_rio_images,
        hero_salvador_images=hero_salvador_images,
        hero_cover=hero_cover,
        categorias=categorias,
        cidades=cidades,
        bairros=bairros,
        meta=meta,
    )


@app.route("/imovel/<string:slug>-id<int:imovel_id>")
def detalhe_imovel(slug, imovel_id):
    imovel = (
        Imovel.query.options(joinedload(Imovel.imagens), joinedload(Imovel.categorias))
        .filter(Imovel.id == imovel_id, Imovel.ativo.is_(True))
        .first_or_404()
    )
    if slug != imovel.slug:
        return redirect(url_for("detalhe_imovel", slug=imovel.slug, imovel_id=imovel.id), code=301)

    meta = {
        "title": f"{imovel.titulo} em {imovel.bairro}, {imovel.cidade}",
        "description": f"{imovel.quartos} quartos, {imovel.suites} suites e {imovel.area_privativa:.0f} m2. Veja fotos, mapa e fale com corretor no WhatsApp.",
        "canonical": url_for("detalhe_imovel", slug=imovel.slug, imovel_id=imovel.id, _external=True),
    }
    return render_template("detalhe.html", imovel=imovel, meta=meta)


@app.route("/api/imoveis/filtrar")
def filtrar_imoveis():
    query = build_imovel_query(request.args)
    imoveis = query.limit(30).all()
    return jsonify({"total": len(imoveis), "items": [i.to_card_json() for i in imoveis]})


@app.route("/api/localidades")
def autocomplete_localidades():
    termo = request.args.get("q", "").strip()
    if len(termo) < 2:
        return jsonify({"items": []})

    termo_norm = normalize_lookup(termo)
    cidades = [cidade for cidade in WORKING_LOCALIDADES if termo_norm in normalize_lookup(cidade)]
    bairros = []
    for lista in WORKING_LOCALIDADES.values():
        bairros.extend([bairro for bairro in lista if termo_norm in normalize_lookup(bairro)])

    result = [{"label": cidade, "tipo": "cidade"} for cidade in cidades[:6]] + [
        {"label": bairro, "tipo": "bairro"} for bairro in bairros[:6]
    ]
    return jsonify({"items": result[:10]})


@app.route("/api/cidades")
def api_cidades():
    termo = request.args.get("q", "").strip()
    cidades = list(WORKING_LOCALIDADES.keys())
    if termo:
        termo_norm = normalize_lookup(termo)
        cidades = [cidade for cidade in cidades if termo_norm in normalize_lookup(cidade)]
    return jsonify({"items": cidades})


@app.route("/api/bairros")
def api_bairros_por_cidade():
    cidade = request.args.get("cidade", "").strip()
    termo = request.args.get("q", "").strip()
    bairros = []
    cidade_map = {normalize_lookup(nome): nome for nome in WORKING_LOCALIDADES}
    cidade_ref = cidade_map.get(normalize_lookup(cidade))
    if cidade_ref:
        bairros = WORKING_LOCALIDADES[cidade_ref]
    if termo:
        termo_norm = normalize_lookup(termo)
        bairros = [bairro for bairro in bairros if termo_norm in normalize_lookup(bairro)]
    return jsonify({"items": bairros})


@app.route("/api/geocode/cep")
def geocode_cep():
    cep = re.sub(r"\D", "", request.args.get("cep", ""))
    if len(cep) != 8:
        return jsonify({"error": "CEP invalido. Use 8 digitos."}), 400

    try:
        with urlopen(f"https://viacep.com.br/ws/{cep}/json/", timeout=10) as response:
            payload_cep = json.loads(response.read().decode("utf-8"))
    except Exception:
        return jsonify({"error": "Falha ao consultar ViaCEP."}), 502

    if payload_cep.get("erro"):
        return jsonify({"error": f"CEP {cep} nao encontrado."}), 404

    logradouro = (payload_cep.get("logradouro") or "").strip()
    bairro = (payload_cep.get("bairro") or "").strip()
    cidade = (payload_cep.get("localidade") or "").strip()
    uf = (payload_cep.get("uf") or "").strip()
    endereco_input = (request.args.get("endereco") or "").strip()
    bairro_input = (request.args.get("bairro") or "").strip()
    cidade_input = (request.args.get("cidade") or "").strip()

    endereco_base = endereco_input or logradouro
    bairro_base = bairro_input or bairro
    cidade_base = cidade_input or cidade

    def search_nominatim(query):
        params = {"format": "jsonv2", "limit": 1, "countrycodes": "br", "q": query}
        nominatim_url = "https://nominatim.openstreetmap.org/search?" + urlencode(params)
        nominatim_request = Request(
            nominatim_url,
            headers={"User-Agent": "meusite-imobiliaria/1.0"},
        )
        with urlopen(nominatim_request, timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))

    queries = []
    full_query = ", ".join(part for part in [endereco_base, bairro_base, cidade_base, uf, cep, "Brasil"] if part)
    if full_query:
        queries.append(full_query)

    cep_query = ", ".join(part for part in [cep, cidade_base, uf, "Brasil"] if part)
    if cep_query and cep_query not in queries:
        queries.append(cep_query)

    bairro_query = ", ".join(part for part in [bairro_base, cidade_base, uf, "Brasil"] if part)
    if bairro_query and bairro_query not in queries:
        queries.append(bairro_query)

    cidade_query = ", ".join(part for part in [cidade_base, uf, "Brasil"] if part)
    if cidade_query and cidade_query not in queries:
        queries.append(cidade_query)

    first = None
    try:
        for query in queries:
            payload_geo = search_nominatim(query)
            if payload_geo:
                first = payload_geo[0]
                break
    except Exception:
        return jsonify({"error": "Falha ao consultar Nominatim."}), 502

    if not first:
        return jsonify(
            {
                "error": f"Geocoding sem resultado para CEP {cep}.",
                "cidade": cidade_base,
                "bairro": bairro_base,
                "endereco_formatado": ", ".join(part for part in [endereco_base or logradouro, bairro_base, cidade_base, uf, cep] if part),
            }
        ), 404

    endereco_formatado = ", ".join(part for part in [endereco_base or logradouro, bairro_base, cidade_base, uf, cep] if part)

    return jsonify(
        {
            "cep": cep,
            "latitude": float(first.get("lat")) if first.get("lat") else None,
            "longitude": float(first.get("lon")) if first.get("lon") else None,
            "endereco_formatado": endereco_formatado,
            "cidade": cidade_base,
            "bairro": bairro_base,
        }
    )


@app.route("/admin/login", methods=["GET", "POST"])
def admin_login():
    admin_user = os.getenv("ADMIN_USER", "reis")
    admin_pass_hash = os.getenv("ADMIN_PASS_HASH", generate_password_hash("Casa@reis7"))
    erro = None

    if request.method == "POST":
        username = request.form.get("username", "")
        password = request.form.get("password", "")
        if username == admin_user and check_password_hash(admin_pass_hash, password):
            session["admin_logged"] = True
            return redirect(request.args.get("next") or url_for("admin"))
        erro = "Credenciais invalidas."

    return render_template("admin_login.html", erro=erro, meta={"title": "Admin Login"})


@app.route("/admin/logout")
def admin_logout():
    session.clear()
    return redirect(url_for("index"))


@app.route("/admin", methods=["GET", "POST"])
@admin_required
def admin():
    categorias = Categoria.query.order_by(Categoria.nome.asc()).all()
    cidades = [r[0] for r in db.session.query(Imovel.cidade).filter(Imovel.cidade.isnot(None)).distinct().order_by(Imovel.cidade.asc()).all() if r[0]]
    bairros = [r[0] for r in db.session.query(Imovel.bairro).filter(Imovel.bairro.isnot(None)).distinct().order_by(Imovel.bairro.asc()).all() if r[0]]
    if request.method == "POST":
        titulo = request.form["titulo"].strip()
        cidade = request.form["cidade"].strip()
        bairro = request.form["bairro"].strip()
        cep = re.sub(r"\D", "", request.form.get("cep", ""))[:8]
        endereco = request.form.get("endereco", "").strip()
        preco = parse_brl_number(request.form.get("preco"), 0)
        descricao = request.form.get("descricao", "").strip()
        info_interna = request.form.get("info_interna", "").strip()
        quartos = request.form.get("quartos", type=int) or 0
        suites = request.form.get("suites", type=int) or 0
        banheiros = request.form.get("banheiros", type=int) or 0
        vagas = request.form.get("vagas", type=int) or 0
        area_privativa = request.form.get("area_privativa", type=float) or 0
        area_total = request.form.get("area_total", type=float)
        iptu = parse_brl_number(request.form.get("iptu"), 0)
        condominio = parse_brl_number(request.form.get("condominio"), 0)
        destaque = request.form.get("destaque") == "on"
        novo = request.form.get("novo") == "on"
        ativo = request.form.get("ativo") == "on"
        latitude = request.form.get("latitude", type=float)
        longitude = request.form.get("longitude", type=float)

        base_slug = slugify(f"{titulo}-{cidade}")
        slug = base_slug
        counter = 2
        while Imovel.query.filter_by(slug=slug).first():
            slug = f"{base_slug}-{counter}"
            counter += 1

        imovel = Imovel(
            slug=slug,
            titulo=titulo,
            descricao=descricao or "Descricao em construcao.",
            info_interna=info_interna,
            cidade=cidade,
            bairro=bairro,
            cep=(f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else ""),
            endereco=endereco,
            preco=preco,
            quartos=quartos,
            suites=suites,
            banheiros=banheiros,
            vagas=vagas,
            area_privativa=area_privativa,
            area_total=area_total,
            iptu=iptu,
            condominio=condominio,
            destaque=destaque,
            novo=novo,
            ativo=ativo,
            latitude=latitude,
            longitude=longitude,
        )

        ids_categorias = request.form.getlist("categorias")
        if ids_categorias:
            imovel.categorias = Categoria.query.filter(Categoria.id.in_(ids_categorias)).all()

        db.session.add(imovel)
        db.session.flush()

        arquivos = request.files.getlist("imagens")
        for idx, arquivo in enumerate(arquivos):
            if not arquivo or not arquivo.filename:
                continue
            static_url = save_uploaded_image(arquivo)
            if not static_url:
                continue
            imagem = ImagemImovel(
                imovel_id=imovel.id,
                ordem=idx,
                eh_capa=idx == 0,
                url_400=static_url,
                url_800=static_url,
                url_1600=static_url,
                webp_800=static_url,
                webp_1600=static_url,
            )
            db.session.add(imagem)

        db.session.commit()
        return redirect(imovel.url_publica)

    imoveis = Imovel.query.order_by(Imovel.created_at.desc()).limit(20).all()
    return render_template(
        "admin.html",
        categorias=categorias,
        imoveis=imoveis,
        cidades=cidades,
        bairros=bairros,
        extrator_origem_padrao=app.config["EXTRATOR_ORIGEM_PADRAO"],
        meta={"title": "Admin de Imoveis"},
    )


@app.route("/admin/importar-extrator", methods=["POST"])
@admin_required
def admin_importar_extrator():
    origem = request.form.get("origem", "").strip() or app.config["EXTRATOR_ORIGEM_PADRAO"]
    limite = request.form.get("limite", type=int) or 0
    atualizar = request.form.get("atualizar") == "on"

    script_path = os.path.join(BASE_DIR, "importar_extrator.py")
    cmd = [sys.executable, script_path, "--origem", origem]
    if limite > 0:
        cmd.extend(["--limite", str(limite)])
    if atualizar:
        cmd.append("--atualizar")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=1800,
            cwd=BASE_DIR,
        )
        output = (result.stdout or result.stderr or "").strip()
        resumo = "\n".join(output.splitlines()[-4:]) if output else "Sem output."

        if result.returncode == 0:
            flash(f"Importacao concluida com sucesso.\n{resumo}", "success")
        else:
            flash(f"Falha na importacao (codigo {result.returncode}).\n{resumo}", "danger")
    except subprocess.TimeoutExpired:
        flash("Importacao excedeu o tempo limite (30 min). Tente com limite menor.", "danger")
    except Exception as exc:
        flash(f"Erro ao executar importacao: {exc}", "danger")

    return redirect(url_for("admin"))


@app.route("/admin/imovel/<int:imovel_id>/editar", methods=["GET", "POST"])
@admin_required
def admin_editar_imovel(imovel_id):
    imovel = (
        Imovel.query.options(joinedload(Imovel.imagens), joinedload(Imovel.categorias))
        .filter_by(id=imovel_id)
        .first_or_404()
    )
    categorias = Categoria.query.order_by(Categoria.nome.asc()).all()
    cidades = [r[0] for r in db.session.query(Imovel.cidade).filter(Imovel.cidade.isnot(None)).distinct().order_by(Imovel.cidade.asc()).all() if r[0]]
    bairros = [r[0] for r in db.session.query(Imovel.bairro).filter(Imovel.bairro.isnot(None)).distinct().order_by(Imovel.bairro.asc()).all() if r[0]]

    if request.method == "POST":
        imovel.titulo = request.form["titulo"].strip()
        imovel.cidade = request.form["cidade"].strip()
        imovel.bairro = request.form["bairro"].strip()
        cep = re.sub(r"\D", "", request.form.get("cep", ""))[:8]
        imovel.cep = f"{cep[:5]}-{cep[5:]}" if len(cep) == 8 else ""
        imovel.endereco = request.form.get("endereco", "").strip()
        imovel.preco = parse_brl_number(request.form.get("preco"), 0)
        imovel.descricao = request.form.get("descricao", "").strip() or "Descricao em construcao."
        imovel.info_interna = request.form.get("info_interna", "").strip()
        imovel.quartos = request.form.get("quartos", type=int) or 0
        imovel.suites = request.form.get("suites", type=int) or 0
        imovel.banheiros = request.form.get("banheiros", type=int) or 0
        imovel.vagas = request.form.get("vagas", type=int) or 0
        imovel.area_privativa = request.form.get("area_privativa", type=float) or 0
        imovel.area_total = request.form.get("area_total", type=float)
        imovel.iptu = parse_brl_number(request.form.get("iptu"), 0)
        imovel.condominio = parse_brl_number(request.form.get("condominio"), 0)
        imovel.destaque = request.form.get("destaque") == "on"
        imovel.novo = request.form.get("novo") == "on"
        imovel.ativo = request.form.get("ativo") == "on"
        imovel.latitude = request.form.get("latitude", type=float)
        imovel.longitude = request.form.get("longitude", type=float)

        desired_slug = slugify(f"{imovel.titulo}-{imovel.cidade}")
        if desired_slug and desired_slug != imovel.slug:
            candidate = desired_slug
            counter = 2
            while Imovel.query.filter(Imovel.slug == candidate, Imovel.id != imovel.id).first():
                candidate = f"{desired_slug}-{counter}"
                counter += 1
            imovel.slug = candidate

        ids_categorias = request.form.getlist("categorias")
        imovel.categorias = (
            Categoria.query.filter(Categoria.id.in_(ids_categorias)).all() if ids_categorias else []
        )

        # Remove imagens selecionadas
        ids_remover = request.form.getlist("remover_imagens")
        if ids_remover:
            images_to_remove = ImagemImovel.query.filter(
                ImagemImovel.imovel_id == imovel.id, ImagemImovel.id.in_(ids_remover)
            ).all()
            for image in images_to_remove:
                cleanup_image_file(image.url_400)
                db.session.delete(image)

        db.session.flush()

        # Upload de novas imagens
        current_count = ImagemImovel.query.filter_by(imovel_id=imovel.id).count()
        arquivos = request.files.getlist("imagens")
        for idx, arquivo in enumerate(arquivos):
            if not arquivo or not arquivo.filename:
                continue
            static_url = save_uploaded_image(arquivo)
            if not static_url:
                continue
            db.session.add(
                ImagemImovel(
                    imovel_id=imovel.id,
                    ordem=current_count + idx,
                    eh_capa=False,
                    url_400=static_url,
                    url_800=static_url,
                    url_1600=static_url,
                    webp_800=static_url,
                    webp_1600=static_url,
                )
            )

        # Define capa
        capa_id = request.form.get("capa_imagem_id", type=int)
        imagens_restantes = (
            ImagemImovel.query.filter_by(imovel_id=imovel.id).order_by(ImagemImovel.ordem.asc()).all()
        )
        if imagens_restantes:
            for image in imagens_restantes:
                image.eh_capa = image.id == capa_id
            if not any(img.eh_capa for img in imagens_restantes):
                imagens_restantes[0].eh_capa = True

            for index, image in enumerate(imagens_restantes):
                image.ordem = index

        db.session.commit()
        return redirect(url_for("admin_editar_imovel", imovel_id=imovel.id))

    return render_template(
        "admin_editar.html",
        imovel=imovel,
        categorias=categorias,
        cidades=cidades,
        bairros=bairros,
        meta={"title": f"Editar {imovel.titulo}"},
    )


@app.route("/admin/imovel/<int:imovel_id>/status", methods=["POST"])
@admin_required
def admin_alterar_status_imovel(imovel_id):
    imovel = Imovel.query.get_or_404(imovel_id)
    imovel.ativo = request.form.get("ativo") == "1"
    db.session.commit()
    return redirect(url_for("admin"))


def seed_data():
    if Categoria.query.count() == 0:
        for nome in ["Venda", "Aluguel", "Lancamentos", "Temporada"]:
            db.session.add(Categoria(nome=nome, slug=slugify(nome), ativa=True))
        db.session.commit()

    if Imovel.query.count() > 0:
        return

    venda = Categoria.query.filter_by(slug="venda").first()
    aluguel = Categoria.query.filter_by(slug="aluguel").first()

    exemplos = [
        {
            "titulo": "Cobertura Panoramica Jardins",
            "descricao": "Cobertura duplex com vista aberta, acabamentos premium e piscina privativa.",
            "cidade": "Sao Paulo",
            "bairro": "Jardins",
            "preco": 4850000,
            "quartos": 4,
            "suites": 3,
            "banheiros": 5,
            "vagas": 4,
            "area_privativa": 365,
            "area_total": 420,
            "iptu": 2200,
            "condominio": 4200,
            "destaque": True,
            "novo": True,
            "categorias": [venda],
            "imagem": "https://images.unsplash.com/photo-1512918728675-ed5a9ecdebfd?q=80&w=1600&auto=format&fit=crop",
            "lat": -23.56144,
            "lng": -46.65588,
        },
        {
            "titulo": "Casa Contemporanea em Condominio Fechado",
            "descricao": "Projeto assinado, automacao completa e area gourmet integrada ao jardim.",
            "cidade": "Campinas",
            "bairro": "Sousas",
            "preco": 3290000,
            "quartos": 5,
            "suites": 5,
            "banheiros": 7,
            "vagas": 6,
            "area_privativa": 510,
            "area_total": 760,
            "iptu": 1900,
            "condominio": 1800,
            "destaque": False,
            "novo": True,
            "categorias": [venda, aluguel],
            "imagem": "https://images.unsplash.com/photo-1600585154340-be6161a56a0c?q=80&w=1600&auto=format&fit=crop",
            "lat": -22.88017,
            "lng": -46.97149,
        },
        {
            "titulo": "Apartamento Alto Padrao Beira Mar",
            "descricao": "Frente mar com varanda ampla, servicos exclusivos e lazer completo.",
            "cidade": "Florianopolis",
            "bairro": "Jurerê Internacional",
            "preco": 2790000,
            "quartos": 3,
            "suites": 2,
            "banheiros": 4,
            "vagas": 2,
            "area_privativa": 188,
            "area_total": 240,
            "iptu": 980,
            "condominio": 1650,
            "destaque": True,
            "novo": False,
            "categorias": [aluguel],
            "imagem": "https://images.unsplash.com/photo-1494526585095-c41746248156?q=80&w=1600&auto=format&fit=crop",
            "lat": -27.43767,
            "lng": -48.49588,
        },
    ]

    for ex in exemplos:
        slug = slugify(f"{ex['titulo']}-{ex['cidade']}")
        imovel = Imovel(
            slug=slug,
            titulo=ex["titulo"],
            descricao=ex["descricao"],
            cidade=ex["cidade"],
            bairro=ex["bairro"],
            preco=ex["preco"],
            quartos=ex["quartos"],
            suites=ex["suites"],
            banheiros=ex["banheiros"],
            vagas=ex["vagas"],
            area_privativa=ex["area_privativa"],
            area_total=ex["area_total"],
            iptu=ex["iptu"],
            condominio=ex["condominio"],
            destaque=ex["destaque"],
            novo=ex["novo"],
            latitude=ex["lat"],
            longitude=ex["lng"],
            ativo=True,
        )
        imovel.categorias = [c for c in ex["categorias"] if c]
        db.session.add(imovel)
        db.session.flush()
        img = ex["imagem"]
        db.session.add(
            ImagemImovel(
                imovel_id=imovel.id,
                ordem=0,
                eh_capa=True,
                url_400=img,
                url_800=img,
                url_1600=img,
                webp_800=img,
                webp_1600=img,
            )
        )
    db.session.commit()


@app.cli.command("init-db")
def init_db():
    db.create_all()
    ensure_optional_columns()
    seed_data()
    print("Banco inicializado com sucesso.")


@app.cli.command("normalize-locations")
def normalize_locations():
    db.create_all()
    ensure_optional_columns()
    itens = Imovel.query.all()
    changed = 0
    for imovel in itens:
        nova_cidade, novo_bairro = normalize_cidade_bairro(imovel.cidade, imovel.bairro)
        if nova_cidade != (imovel.cidade or "") or novo_bairro != (imovel.bairro or ""):
            imovel.cidade = nova_cidade
            imovel.bairro = novo_bairro
            changed += 1
    db.session.commit()
    print(f"Normalizacao concluida. Registros alterados: {changed}")


if __name__ == "__main__":
    with app.app_context():
        db.create_all()
        ensure_optional_columns()
        seed_data()
    app.run(debug=True)
