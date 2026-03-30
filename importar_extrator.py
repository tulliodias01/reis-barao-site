import argparse
import os
import re
import shutil
import unicodedata
from pathlib import Path

from app import (
    Categoria,
    ImagemImovel,
    Imovel,
    app,
    db,
    ensure_optional_columns,
    normalize_cidade_bairro,
    slugify,
)


DEFAULT_ORIGEM = r"D:\BACKUP IMOBILIARIA\PDF EXTRATOR\ACREDITE SONHOS  IMOVEIS ORGANIZADO\IMOVEIS"
IMAGE_EXTS = {".jpg", ".jpeg", ".png", ".webp", ".bmp", ".tiff"}
IGNORED_DIRS = {"INBOX_IMPORTADAS", "__pycache__"}


def read_text_fallback(path: Path) -> str:
    for enc in ("utf-8", "utf-8-sig", "cp1252", "latin-1"):
        try:
            return path.read_text(encoding=enc)
        except UnicodeDecodeError:
            continue
    return path.read_text(encoding="utf-8", errors="ignore")


def weird_score(text: str) -> int:
    markers = ["Ã", "Â", "â€œ", "â€", "â€™", "�", "Ð", "¤"]
    return sum(text.count(m) for m in markers)


def clean_mojibake(text: str) -> str:
    if not text:
        return text

    candidates = [text]
    for src_enc in ("latin-1", "cp1252"):
        try:
            candidates.append(text.encode(src_enc).decode("utf-8"))
        except UnicodeError:
            pass

    repaired = min(candidates, key=weird_score)
    repaired = repaired.replace("\ufeff", "").replace("\xa0", " ").strip()
    return repaired


def normalize_key(key: str) -> str:
    key = clean_mojibake(key).strip().lower()
    key = unicodedata.normalize("NFKD", key).encode("ascii", "ignore").decode("ascii")
    return key


def parse_dados(path: Path) -> dict:
    data = {}
    if not path.exists():
        return data

    raw = clean_mojibake(read_text_fallback(path))
    for line in raw.splitlines():
        if ":" not in line:
            continue
        key, value = line.split(":", 1)
        data[normalize_key(key)] = clean_mojibake(value).strip()
    return data


def to_float(value: str) -> float | None:
    if not value:
        return None
    v = clean_mojibake(value).strip().lower()
    if v in {"-", "--", "***", "**", "n/a", "na"}:
        return None

    if "mil" in v:
        m = re.search(r"(\d+[.,]?\d*)", v)
        if m:
            return float(m.group(1).replace(",", ".")) * 1000

    cleaned = re.sub(r"[^\d,.-]", "", v)
    if not cleaned:
        return None

    if "," in cleaned and "." in cleaned:
        cleaned = cleaned.replace(".", "").replace(",", ".")
    elif "," in cleaned:
        cleaned = cleaned.replace(",", ".")
    elif "." in cleaned and cleaned.count(".") > 0:
        parts = cleaned.split(".")
        if all(p.isdigit() for p in parts) and len(parts[-1]) == 3:
            cleaned = "".join(parts)

    try:
        return float(cleaned)
    except ValueError:
        return None


def to_int(value: str) -> int:
    val = to_float(value)
    return int(val) if val is not None else 0


def parse_area(value: str) -> float | None:
    if not value:
        return None
    m = re.search(r"(\d+[.,]?\d*)", clean_mojibake(value))
    if not m:
        return None
    return float(m.group(1).replace(",", "."))


def extract_first_int(pattern: str, text: str) -> int | None:
    if not text:
        return None
    m = re.search(pattern, clean_mojibake(text), flags=re.IGNORECASE)
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def parse_money_from_text(label: str, text: str) -> float | None:
    if not text:
        return None
    pattern = rf"{label}\s*[:\-]?\s*R?\$?\s*([\d\.\,]+)"
    m = re.search(pattern, clean_mojibake(text), flags=re.IGNORECASE)
    if not m:
        return None
    return to_float(m.group(1))


def humanize_dir_name(name: str) -> str:
    base = re.sub(r"^IMOVEL_\d+_?", "", name, flags=re.IGNORECASE)
    base = base.replace("_", " ").strip()
    return clean_mojibake(base).title() if base else "Imovel"


def collect_images(imovel_dir: Path) -> list[Path]:
    imgs = []
    for root, dirs, files in os.walk(imovel_dir):
        dirs[:] = [d for d in dirs if d not in IGNORED_DIRS]
        root_path = Path(root)
        for filename in files:
            ext = Path(filename).suffix.lower()
            if ext not in IMAGE_EXTS:
                continue
            imgs.append(root_path / filename)

    imgs.sort(key=lambda p: (0 if "FACHADA" in str(p).upper() else 1, str(p).upper()))
    return imgs


def ensure_categorias():
    defaults = [
        ("Venda", "venda"),
        ("Aluguel", "aluguel"),
        ("Lancamentos", "lancamentos"),
        ("Temporada", "temporada"),
    ]
    for nome, slug in defaults:
        if not Categoria.query.filter_by(slug=slug).first():
            db.session.add(Categoria(nome=nome, slug=slug, ativa=True))
    db.session.commit()


def find_categoria_slug(negocio: str) -> str:
    n = clean_mojibake((negocio or "")).lower()
    if "alugu" in n:
        return "aluguel"
    if "tempor" in n:
        return "temporada"
    if "lanca" in n:
        return "lancamentos"
    return "venda"


def import_one(imovel_dir: Path, origem_base: Path, atualizar: bool = False) -> tuple[str, str]:
    dados = parse_dados(imovel_dir / "dados_imovel.txt")
    descricao = (
        clean_mojibake(read_text_fallback(imovel_dir / "descricao.txt"))
        if (imovel_dir / "descricao.txt").exists()
        else ""
    )
    info = (
        clean_mojibake(read_text_fallback(imovel_dir / "info.txt"))
        if (imovel_dir / "info.txt").exists()
        else ""
    )

    codigo = (dados.get("prefixo") or imovel_dir.name).strip()
    codigo_origem = slugify(codigo)[:120]
    tipo = dados.get("tipo") or "Imovel"
    negocio = dados.get("negocio") or "Venda"
    cidade_raw = imovel_dir.parent.name if imovel_dir.parent != origem_base else (dados.get("localizacao") or imovel_dir.name)
    cidade = clean_mojibake(cidade_raw.replace("_", " ")).title()
    bairro = clean_mojibake((dados.get("localizacao") or cidade)).title()
    cidade, bairro = normalize_cidade_bairro(cidade, bairro)
    titulo_base = humanize_dir_name(imovel_dir.name)
    titulo = f"{tipo.title()} {titulo_base}" if titulo_base.lower() not in tipo.lower() else titulo_base

    preco = to_float(dados.get("valor", "")) or 0
    quartos = to_int(dados.get("quartos", ""))
    area = parse_area(dados.get("metragem", "")) or 0
    suites = extract_first_int(r"(\d+)\s*su[ii]tes?", descricao) or 0
    banheiros = extract_first_int(r"(\d+)\s*banheiros?", descricao) or 0
    vagas = extract_first_int(r"(\d+)\s*vagas?", descricao) or 0
    iptu = parse_money_from_text("IPTU", descricao) or 0
    condominio = parse_money_from_text("Condom[ii]nio|Condominio|Condom[ií]nio", descricao) or 0
    full_desc = descricao.strip() or f"Descricao nao informada no arquivo descricao.txt ({imovel_dir.name})."

    # Chave forte anti-duplicacao do extrator.
    imovel = None
    if codigo_origem:
        imovel = Imovel.query.filter_by(codigo_origem=codigo_origem).first()
        if not imovel:
            imovel = (
                Imovel.query.filter(Imovel.slug.like(f"{codigo_origem}-%"))
                .order_by(Imovel.id.asc())
                .first()
            )

    base_slug = slugify(f"{codigo}-{cidade}")
    slug = imovel.slug if imovel else base_slug
    if not imovel:
        idx = 2
        while Imovel.query.filter_by(slug=slug).first():
            slug = f"{base_slug}-{idx}"
            idx += 1

    if imovel and not atualizar:
        return ("skip", f"{imovel_dir.name} -> ja existe (slug={slug})")

    if not imovel:
        imovel = Imovel(slug=slug, ativo=True)
        db.session.add(imovel)

    imovel.codigo_origem = codigo_origem or None
    imovel.titulo = titulo[:180]
    imovel.descricao = full_desc
    imovel.info_interna = info.strip()
    imovel.cidade = cidade[:120]
    imovel.bairro = bairro[:120]
    imovel.preco = preco
    imovel.quartos = quartos
    imovel.suites = suites
    imovel.banheiros = banheiros
    imovel.vagas = vagas
    imovel.area_privativa = area if area > 0 else 1
    imovel.area_total = area if area > 0 else None
    imovel.iptu = iptu
    imovel.condominio = condominio
    imovel.novo = False
    imovel.destaque = "alto padrao" in full_desc.lower() or "luxo" in full_desc.lower()

    categoria = Categoria.query.filter_by(slug=find_categoria_slug(negocio)).first()
    if categoria:
        imovel.categorias = [categoria]

    db.session.flush()

    target_base = Path(app.static_folder) / "uploads" / "importados" / imovel.slug
    if atualizar:
        for img in list(imovel.imagens):
            db.session.delete(img)
        if target_base.exists():
            shutil.rmtree(target_base, ignore_errors=True)

    imagens = collect_images(imovel_dir)
    target_base.mkdir(parents=True, exist_ok=True)

    for i, src in enumerate(imagens):
        dest = target_base / f"{i:04d}_{src.name}"
        if not dest.exists():
            shutil.copy2(src, dest)
        relative = str(dest.relative_to(Path(app.static_folder))).replace("\\", "/")
        url = f"/static/{relative}"
        db.session.add(
            ImagemImovel(
                imovel_id=imovel.id,
                ordem=i,
                eh_capa=i == 0,
                url_400=url,
                url_800=url,
                url_1600=url,
                webp_800=url,
                webp_1600=url,
            )
        )

    return ("ok", f"{imovel_dir.name} -> importado ({len(imagens)} imagens)")


def run_import(origem: str, limite: int = 0, atualizar: bool = False):
    origem_base = Path(origem)
    if not origem_base.exists():
        raise FileNotFoundError(f"Pasta de origem nao encontrada: {origem}")

    with app.app_context():
        db.create_all()
        ensure_optional_columns()
        ensure_categorias()

        all_candidates = sorted({p.parent for p in origem_base.rglob("dados_imovel.txt")})
        candidates = [d for d in all_candidates if d.name.upper().startswith("IMOVEL_")]
        if not candidates:
            candidates = all_candidates
        if limite > 0:
            candidates = candidates[:limite]

        ok = skip = err = 0
        for imovel_dir in candidates:
            try:
                status, msg = import_one(imovel_dir, origem_base, atualizar=atualizar)
                print(f"[{status.upper()}] {msg}")
                if status == "ok":
                    ok += 1
                elif status == "skip":
                    skip += 1
            except Exception as exc:
                err += 1
                print(f"[ERRO] {imovel_dir}: {exc}")

        db.session.commit()
        print(f"\nResumo: ok={ok} skip={skip} erro={err}")


def parse_args():
    parser = argparse.ArgumentParser(description="Importa imoveis do PDF Extrator para o site Flask.")
    parser.add_argument("--origem", default=DEFAULT_ORIGEM, help="Pasta IMOVEIS do extrator.")
    parser.add_argument("--limite", type=int, default=0, help="Quantidade maxima de imoveis para importar (0 = todos).")
    parser.add_argument("--atualizar", action="store_true", help="Atualiza imoveis existentes e recria galeria.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run_import(origem=args.origem, limite=args.limite, atualizar=args.atualizar)
