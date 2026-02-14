"""
DIY Investing — Full Stack Backend v6
======================================
- PostgreSQL for users, subscriptions, watchlists
- JWT auth + Google OAuth
- Razorpay subscriptions (monthly, 3-month, yearly)
- Admin dashboard API
- yfinance for stock data (INR native)
"""

import os, time, math, logging, hashlib, secrets, json
from datetime import datetime, timedelta
from functools import wraps

from flask import Flask, request, jsonify, redirect, url_for, g
from flask_cors import CORS
from flask_sqlalchemy import SQLAlchemy
import jwt as pyjwt
import requests
import yfinance as yf

# ═══════ CONFIG ═══════
app = Flask(__name__)
CORS(app, supports_credentials=True)

app.config["SECRET_KEY"] = os.environ.get("SECRET_KEY", secrets.token_hex(32))
app.config["SQLALCHEMY_DATABASE_URI"] = os.environ.get("DATABASE_URL", "sqlite:///diy.db")
# Fix Render's postgres:// vs postgresql://
if app.config["SQLALCHEMY_DATABASE_URI"].startswith("postgres://"):
    app.config["SQLALCHEMY_DATABASE_URI"] = app.config["SQLALCHEMY_DATABASE_URI"].replace("postgres://", "postgresql://", 1)
app.config["SQLALCHEMY_TRACK_MODIFICATIONS"] = False

db = SQLAlchemy(app)

PORT = int(os.environ.get("PORT", 10000))
GOOGLE_CLIENT_ID = os.environ.get("GOOGLE_CLIENT_ID", "")
GOOGLE_CLIENT_SECRET = os.environ.get("GOOGLE_CLIENT_SECRET", "")
RAZORPAY_KEY_ID = os.environ.get("RAZORPAY_KEY_ID", "")
RAZORPAY_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
ADMIN_EMAILS = os.environ.get("ADMIN_EMAILS", "").split(",")  # comma-separated
FRONTEND_URL = os.environ.get("FRONTEND_URL", "https://diyinvesting.in")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
log = logging.getLogger("diy")


# ═══════ DATABASE MODELS ═══════

class User(db.Model):
    __tablename__ = "users"
    id = db.Column(db.Integer, primary_key=True)
    email = db.Column(db.String(255), unique=True, nullable=False, index=True)
    name = db.Column(db.String(255), default="")
    phone = db.Column(db.String(20), default="")
    password_hash = db.Column(db.String(255), default="")  # empty for Google-only users
    google_id = db.Column(db.String(255), default="")
    avatar_url = db.Column(db.String(500), default="")
    is_admin = db.Column(db.Boolean, default=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    last_login = db.Column(db.DateTime, default=datetime.utcnow)
    # Subscription
    plan = db.Column(db.String(20), default="free")  # free, trial, monthly, quarterly, yearly
    plan_expires = db.Column(db.DateTime, nullable=True)
    razorpay_payment_id = db.Column(db.String(255), default="")
    razorpay_order_id = db.Column(db.String(255), default="")
    total_paid = db.Column(db.Float, default=0)
    # Engagement
    login_count = db.Column(db.Integer, default=0)
    stocks_analyzed = db.Column(db.Integer, default=0)

    watchlist = db.relationship("WatchlistItem", backref="user", lazy=True, cascade="all, delete-orphan")
    payments = db.relationship("Payment", backref="user", lazy=True, cascade="all, delete-orphan")

    def is_pro(self):
        if self.is_admin:
            return True
        if self.plan == "free":
            return False
        if self.plan == "trial":
            return self.plan_expires and datetime.utcnow() < self.plan_expires
        return self.plan_expires and datetime.utcnow() < self.plan_expires

    def days_left(self):
        if not self.plan_expires:
            return 0
        d = (self.plan_expires - datetime.utcnow()).days
        return max(0, d)

    def to_dict(self, include_private=False):
        d = {
            "id": self.id,
            "email": self.email,
            "name": self.name,
            "phone": self.phone,
            "avatar": self.avatar_url,
            "isAdmin": self.is_admin,
            "plan": self.plan,
            "isPro": self.is_pro(),
            "daysLeft": self.days_left(),
            "planExpires": self.plan_expires.isoformat() if self.plan_expires else None,
            "createdAt": self.created_at.isoformat(),
            "hasGoogle": bool(self.google_id),
            "hasPassword": bool(self.password_hash),
        }
        if include_private:
            d["loginCount"] = self.login_count
            d["stocksAnalyzed"] = self.stocks_analyzed
            d["totalPaid"] = self.total_paid
            d["lastLogin"] = self.last_login.isoformat() if self.last_login else None
        return d


class WatchlistItem(db.Model):
    __tablename__ = "watchlist"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    symbol = db.Column(db.String(30), nullable=False)
    name = db.Column(db.String(255), default="")
    sector = db.Column(db.String(100), default="")
    cmp = db.Column(db.Float, default=0)
    mcap = db.Column(db.Float, default=0)
    implied_growth = db.Column(db.Float, nullable=True)
    intrinsic_value = db.Column(db.Float, default=0)
    gap = db.Column(db.Float, default=0)
    # User's assumptions
    exit_pe = db.Column(db.Float, default=20)
    discount_rate = db.Column(db.Float, default=15)
    forecast_years = db.Column(db.Integer, default=10)
    expected_cagr = db.Column(db.Float, default=15)
    added_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    def to_dict(self):
        return {
            "id": self.id,
            "sym": self.symbol,
            "name": self.name,
            "sec": self.sector,
            "cmp": self.cmp,
            "mcap": self.mcap,
            "ig": self.implied_growth,
            "iv": self.intrinsic_value,
            "gap": self.gap,
            "inputs": {
                "pe": self.exit_pe,
                "dr": self.discount_rate,
                "fy": self.forecast_years,
                "ec": self.expected_cagr,
            },
            "addedAt": self.added_at.isoformat(),
        }


class Payment(db.Model):
    __tablename__ = "payments"
    id = db.Column(db.Integer, primary_key=True)
    user_id = db.Column(db.Integer, db.ForeignKey("users.id"), nullable=False)
    razorpay_payment_id = db.Column(db.String(255), default="")
    razorpay_order_id = db.Column(db.String(255), default="")
    razorpay_signature = db.Column(db.String(500), default="")
    amount = db.Column(db.Float, default=0)
    plan = db.Column(db.String(20), default="")
    status = db.Column(db.String(20), default="pending")
    created_at = db.Column(db.DateTime, default=datetime.utcnow)


# ═══════ AUTH HELPERS ═══════

def hash_password(pwd):
    salt = secrets.token_hex(16)
    h = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt.encode(), 200000)
    return salt + ":" + h.hex()

def verify_password(pwd, stored):
    if not stored or ":" not in stored:
        return False
    salt, h = stored.split(":", 1)
    h2 = hashlib.pbkdf2_hmac("sha256", pwd.encode(), salt.encode(), 200000)
    return h == h2.hex()

def make_token(user):
    payload = {
        "uid": user.id,
        "email": user.email,
        "admin": user.is_admin,
        "exp": datetime.utcnow() + timedelta(days=30),
    }
    return pyjwt.encode(payload, app.config["SECRET_KEY"], algorithm="HS256")

def auth_required(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        token = None
        auth = request.headers.get("Authorization", "")
        if auth.startswith("Bearer "):
            token = auth[7:]
        if not token:
            return jsonify({"error": "Login required"}), 401
        try:
            data = pyjwt.decode(token, app.config["SECRET_KEY"], algorithms=["HS256"])
            g.user = User.query.get(data["uid"])
            if not g.user:
                return jsonify({"error": "User not found"}), 401
        except pyjwt.ExpiredSignatureError:
            return jsonify({"error": "Token expired, please login again"}), 401
        except Exception:
            return jsonify({"error": "Invalid token"}), 401
        return f(*args, **kwargs)
    return decorated

def admin_required(f):
    @wraps(f)
    @auth_required
    def decorated(*args, **kwargs):
        if not g.user.is_admin:
            return jsonify({"error": "Admin access required"}), 403
        return f(*args, **kwargs)
    return decorated


# ═══════ AUTH ROUTES ═══════

@app.route("/api/auth/signup", methods=["POST"])
def auth_signup():
    d = request.json or {}
    email = d.get("email", "").strip().lower()
    pwd = d.get("password", "")
    name = d.get("name", "").strip()
    phone = d.get("phone", "").strip()

    if not email or not pwd:
        return jsonify({"error": "Email and password required"}), 400
    if len(pwd) < 6:
        return jsonify({"error": "Password must be at least 6 characters"}), 400
    if User.query.filter_by(email=email).first():
        return jsonify({"error": "Account already exists. Please login."}), 400

    u = User(
        email=email,
        name=name,
        phone=phone,
        password_hash=hash_password(pwd),
        plan="trial",
        plan_expires=datetime.utcnow() + timedelta(days=2),
        is_admin=email in ADMIN_EMAILS,
        login_count=1,
        last_login=datetime.utcnow(),
    )
    db.session.add(u)
    db.session.commit()

    return jsonify({"token": make_token(u), "user": u.to_dict()})


@app.route("/api/auth/login", methods=["POST"])
def auth_login():
    d = request.json or {}
    email = d.get("email", "").strip().lower()
    pwd = d.get("password", "")

    if not email or not pwd:
        return jsonify({"error": "Email and password required"}), 400

    u = User.query.filter_by(email=email).first()
    if not u:
        return jsonify({"error": "No account found with this email"}), 401
    if not verify_password(pwd, u.password_hash):
        return jsonify({"error": "Wrong password"}), 401

    u.login_count += 1
    u.last_login = datetime.utcnow()
    db.session.commit()

    return jsonify({"token": make_token(u), "user": u.to_dict()})


@app.route("/api/auth/google", methods=["POST"])
def auth_google():
    """Handle Google OAuth token from frontend."""
    d = request.json or {}
    credential = d.get("credential", "")
    if not credential:
        return jsonify({"error": "Missing Google credential"}), 400

    # Verify token with Google
    try:
        resp = requests.get(
            f"https://oauth2.googleapis.com/tokeninfo?id_token={credential}",
            timeout=10
        )
        if resp.status_code != 200:
            return jsonify({"error": "Invalid Google token"}), 401
        gdata = resp.json()
    except Exception as e:
        return jsonify({"error": f"Google verification failed: {str(e)}"}), 500

    email = gdata.get("email", "").lower()
    google_id = gdata.get("sub", "")
    name = gdata.get("name", "")
    avatar = gdata.get("picture", "")

    if not email:
        return jsonify({"error": "Could not get email from Google"}), 400

    # Find or create user
    u = User.query.filter_by(email=email).first()
    if u:
        # Existing user — link Google if not linked
        if not u.google_id:
            u.google_id = google_id
        u.avatar_url = avatar or u.avatar_url
        u.name = name or u.name
        u.login_count += 1
        u.last_login = datetime.utcnow()
    else:
        # New user
        u = User(
            email=email,
            name=name,
            google_id=google_id,
            avatar_url=avatar,
            plan="trial",
            plan_expires=datetime.utcnow() + timedelta(days=2),
            is_admin=email in ADMIN_EMAILS,
            login_count=1,
            last_login=datetime.utcnow(),
        )
        db.session.add(u)

    db.session.commit()
    return jsonify({"token": make_token(u), "user": u.to_dict()})


@app.route("/api/auth/me")
@auth_required
def auth_me():
    return jsonify({"user": g.user.to_dict()})


@app.route("/api/auth/profile", methods=["PUT"])
@auth_required
def update_profile():
    d = request.json or {}
    u = g.user
    if "name" in d:
        u.name = d["name"].strip()
    if "phone" in d:
        u.phone = d["phone"].strip()
    if "email" in d and d["email"].strip().lower() != u.email:
        new_email = d["email"].strip().lower()
        if User.query.filter_by(email=new_email).first():
            return jsonify({"error": "Email already in use"}), 400
        u.email = new_email
    if "password" in d and d["password"]:
        if len(d["password"]) < 6:
            return jsonify({"error": "Password must be at least 6 characters"}), 400
        u.password_hash = hash_password(d["password"])

    db.session.commit()
    return jsonify({"user": u.to_dict()})


# ═══════ RAZORPAY ROUTES ═══════

PLANS = {
    "monthly":   {"amount": 4900,  "label": "Monthly",    "days": 30},
    "quarterly": {"amount": 11900, "label": "3-Month",    "days": 90},
    "yearly":    {"amount": 44900, "label": "Annual",     "days": 365},
}

@app.route("/api/payment/create-order", methods=["POST"])
@auth_required
def create_order():
    d = request.json or {}
    plan_key = d.get("plan", "monthly")
    if plan_key not in PLANS:
        return jsonify({"error": "Invalid plan"}), 400

    plan = PLANS[plan_key]

    if not RAZORPAY_KEY_ID or not RAZORPAY_KEY_SECRET:
        return jsonify({"error": "Payment not configured. Contact support."}), 500

    # Create Razorpay order
    try:
        resp = requests.post(
            "https://api.razorpay.com/v1/orders",
            auth=(RAZORPAY_KEY_ID, RAZORPAY_KEY_SECRET),
            json={
                "amount": plan["amount"],
                "currency": "INR",
                "receipt": f"diy_{g.user.id}_{plan_key}_{int(time.time())}",
                "notes": {
                    "user_id": str(g.user.id),
                    "email": g.user.email,
                    "plan": plan_key,
                }
            },
            timeout=10
        )
        order = resp.json()
    except Exception as e:
        return jsonify({"error": f"Razorpay error: {str(e)}"}), 500

    # Save pending payment
    p = Payment(
        user_id=g.user.id,
        razorpay_order_id=order.get("id", ""),
        amount=plan["amount"] / 100,
        plan=plan_key,
        status="pending"
    )
    db.session.add(p)
    db.session.commit()

    return jsonify({
        "orderId": order.get("id"),
        "amount": plan["amount"],
        "currency": "INR",
        "key": RAZORPAY_KEY_ID,
        "plan": plan_key,
        "label": plan["label"],
    })


@app.route("/api/payment/verify", methods=["POST"])
@auth_required
def verify_payment():
    d = request.json or {}
    order_id = d.get("razorpay_order_id", "")
    payment_id = d.get("razorpay_payment_id", "")
    signature = d.get("razorpay_signature", "")
    plan_key = d.get("plan", "monthly")

    if not order_id or not payment_id or not signature:
        return jsonify({"error": "Missing payment details"}), 400

    # Verify signature
    import hmac
    expected = hmac.new(
        RAZORPAY_KEY_SECRET.encode(),
        f"{order_id}|{payment_id}".encode(),
        hashlib.sha256
    ).hexdigest()

    if expected != signature:
        return jsonify({"error": "Payment verification failed"}), 400

    # Update payment record
    p = Payment.query.filter_by(razorpay_order_id=order_id, user_id=g.user.id).first()
    if p:
        p.razorpay_payment_id = payment_id
        p.razorpay_signature = signature
        p.status = "paid"

    # Activate plan
    plan = PLANS.get(plan_key, PLANS["monthly"])
    u = g.user
    # If already pro, extend from current expiry
    if u.plan_expires and u.plan_expires > datetime.utcnow():
        u.plan_expires = u.plan_expires + timedelta(days=plan["days"])
    else:
        u.plan_expires = datetime.utcnow() + timedelta(days=plan["days"])
    u.plan = plan_key
    u.razorpay_payment_id = payment_id
    u.razorpay_order_id = order_id
    u.total_paid = (u.total_paid or 0) + plan["amount"] / 100

    db.session.commit()
    log.info(f"[PAYMENT] {u.email} → {plan_key} ({plan['amount']/100}₹) until {u.plan_expires}")

    return jsonify({"user": u.to_dict(), "message": "Payment successful!"})


# ═══════ WATCHLIST ROUTES (Server-side, synced across devices) ═══════

@app.route("/api/watchlist")
@auth_required
def get_watchlist():
    items = WatchlistItem.query.filter_by(user_id=g.user.id).order_by(WatchlistItem.added_at.desc()).all()
    return jsonify([i.to_dict() for i in items])


@app.route("/api/watchlist", methods=["POST"])
@auth_required
def add_to_watchlist():
    d = request.json or {}
    sym = d.get("sym", "").upper()
    if not sym:
        return jsonify({"error": "Symbol required"}), 400

    # Check if already in watchlist
    existing = WatchlistItem.query.filter_by(user_id=g.user.id, symbol=sym).first()
    if existing:
        # Update
        existing.cmp = d.get("cmp", existing.cmp)
        existing.mcap = d.get("mcap", existing.mcap)
        existing.implied_growth = d.get("ig")
        existing.intrinsic_value = d.get("iv", existing.intrinsic_value)
        existing.gap = d.get("gap", existing.gap)
        inputs = d.get("inputs", {})
        existing.exit_pe = inputs.get("pe", existing.exit_pe)
        existing.discount_rate = inputs.get("dr", existing.discount_rate)
        existing.forecast_years = inputs.get("fy", existing.forecast_years)
        existing.expected_cagr = inputs.get("ec", existing.expected_cagr)
    else:
        inputs = d.get("inputs", {})
        item = WatchlistItem(
            user_id=g.user.id,
            symbol=sym,
            name=d.get("name", ""),
            sector=d.get("sec", ""),
            cmp=d.get("cmp", 0),
            mcap=d.get("mcap", 0),
            implied_growth=d.get("ig"),
            intrinsic_value=d.get("iv", 0),
            gap=d.get("gap", 0),
            exit_pe=inputs.get("pe", 20),
            discount_rate=inputs.get("dr", 15),
            forecast_years=inputs.get("fy", 10),
            expected_cagr=inputs.get("ec", 15),
        )
        db.session.add(item)

    db.session.commit()
    items = WatchlistItem.query.filter_by(user_id=g.user.id).order_by(WatchlistItem.added_at.desc()).all()
    return jsonify([i.to_dict() for i in items])


@app.route("/api/watchlist/<int:item_id>", methods=["DELETE"])
@auth_required
def remove_from_watchlist(item_id):
    item = WatchlistItem.query.filter_by(id=item_id, user_id=g.user.id).first()
    if item:
        db.session.delete(item)
        db.session.commit()
    items = WatchlistItem.query.filter_by(user_id=g.user.id).order_by(WatchlistItem.added_at.desc()).all()
    return jsonify([i.to_dict() for i in items])


@app.route("/api/watchlist/<sym>/update", methods=["PUT"])
@auth_required
def update_watchlist_item(sym):
    d = request.json or {}
    item = WatchlistItem.query.filter_by(user_id=g.user.id, symbol=sym.upper()).first()
    if not item:
        return jsonify({"error": "Item not found"}), 404

    inputs = d.get("inputs", {})
    if "pe" in inputs: item.exit_pe = inputs["pe"]
    if "dr" in inputs: item.discount_rate = inputs["dr"]
    if "fy" in inputs: item.forecast_years = inputs["fy"]
    if "ec" in inputs: item.expected_cagr = inputs["ec"]
    if "ig" in d: item.implied_growth = d["ig"]
    if "iv" in d: item.intrinsic_value = d["iv"]
    if "gap" in d: item.gap = d["gap"]
    if "cmp" in d: item.cmp = d["cmp"]
    if "mcap" in d: item.mcap = d["mcap"]

    db.session.commit()
    return jsonify(item.to_dict())


# ═══════ ADMIN ROUTES ═══════

@app.route("/api/admin/users")
@admin_required
def admin_users():
    users = User.query.order_by(User.created_at.desc()).all()
    return jsonify({
        "total": len(users),
        "pro": sum(1 for u in users if u.is_pro()),
        "trial": sum(1 for u in users if u.plan == "trial"),
        "free": sum(1 for u in users if u.plan == "free"),
        "revenue": sum(u.total_paid or 0 for u in users),
        "users": [u.to_dict(include_private=True) for u in users]
    })


@app.route("/api/admin/payments")
@admin_required
def admin_payments():
    payments = Payment.query.filter_by(status="paid").order_by(Payment.created_at.desc()).all()
    result = []
    for p in payments:
        u = User.query.get(p.user_id)
        result.append({
            "id": p.id,
            "email": u.email if u else "?",
            "name": u.name if u else "?",
            "amount": p.amount,
            "plan": p.plan,
            "paymentId": p.razorpay_payment_id,
            "date": p.created_at.isoformat(),
        })
    return jsonify({"payments": result, "total_revenue": sum(p["amount"] for p in result)})


@app.route("/api/admin/stats")
@admin_required
def admin_stats():
    users = User.query.all()
    now = datetime.utcnow()
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    week_ago = now - timedelta(days=7)
    month_ago = now - timedelta(days=30)

    return jsonify({
        "totalUsers": len(users),
        "proUsers": sum(1 for u in users if u.is_pro() and not u.is_admin),
        "trialUsers": sum(1 for u in users if u.plan == "trial"),
        "activeToday": sum(1 for u in users if u.last_login and u.last_login >= today),
        "activeWeek": sum(1 for u in users if u.last_login and u.last_login >= week_ago),
        "activeMonth": sum(1 for u in users if u.last_login and u.last_login >= month_ago),
        "newThisWeek": sum(1 for u in users if u.created_at >= week_ago),
        "newThisMonth": sum(1 for u in users if u.created_at >= month_ago),
        "totalRevenue": sum(u.total_paid or 0 for u in users),
        "totalStocksAnalyzed": sum(u.stocks_analyzed or 0 for u in users),
        "avgLoginsPerUser": round(sum(u.login_count or 0 for u in users) / max(len(users), 1), 1),
        "planBreakdown": {
            "monthly": sum(1 for u in users if u.plan == "monthly"),
            "quarterly": sum(1 for u in users if u.plan == "quarterly"),
            "yearly": sum(1 for u in users if u.plan == "yearly"),
        }
    })


@app.route("/api/admin/user/<int:uid>/toggle-pro", methods=["POST"])
@admin_required
def admin_toggle_pro(uid):
    u = User.query.get(uid)
    if not u:
        return jsonify({"error": "User not found"}), 404
    if u.is_pro():
        u.plan = "free"
        u.plan_expires = None
    else:
        u.plan = "yearly"
        u.plan_expires = datetime.utcnow() + timedelta(days=365)
    db.session.commit()
    return jsonify({"user": u.to_dict(include_private=True)})


# ═══════ STOCK DATA (yfinance) ═══════

cache = {}
QUOTE_TTL = 300
FIN_TTL = 86400
SEARCH_TTL = 86400

def cached(key, ttl=300):
    e = cache.get(key)
    if not e: return None
    if time.time() - e["t"] > ttl:
        del cache[key]
        return None
    return e["d"]

def set_cache(key, data):
    cache[key] = {"d": data, "t": time.time()}


def yf_quote(symbol):
    ticker = symbol if "." in symbol else symbol + ".NS"
    try:
        t = yf.Ticker(ticker)
        info = t.info
        if not info or not info.get("regularMarketPrice"):
            if not ticker.endswith(".BO"):
                ticker2 = symbol.replace(".NS", "") + ".BO"
                t = yf.Ticker(ticker2)
                info = t.info
                if not info or not info.get("regularMarketPrice"):
                    return None
        cmp = info.get("regularMarketPrice") or info.get("currentPrice") or 0
        mcap = info.get("marketCap", 0)
        pe = info.get("trailingPE") or info.get("forwardPE") or 0
        eps = info.get("trailingEps", 0)
        shares = info.get("sharesOutstanding", 0)
        return {
            "cmp": round(cmp, 2), "mcap_raw": mcap,
            "mcap_cr": round(mcap / 1e7, 0) if mcap > 0 else 0,
            "pe": round(pe, 2) if pe else 0,
            "eps": round(eps, 2) if eps else 0,
            "shares_cr": round(shares / 1e7, 2) if shares else 0,
            "name": info.get("longName") or info.get("shortName") or symbol,
            "sector": info.get("sector", ""),
            "industry": info.get("industry", ""),
            "change": round(info.get("regularMarketChange", 0), 2),
            "changePct": round((info.get("regularMarketChangePercent", 0) or 0) * 100, 2),
            "yearHigh": info.get("fiftyTwoWeekHigh", 0),
            "yearLow": info.get("fiftyTwoWeekLow", 0),
            "bookValue": info.get("bookValue", 0),
            "dividendYield": round((info.get("dividendYield", 0) or 0) * 100, 2),
            "currency": info.get("currency", "INR"),
        }
    except Exception as e:
        log.error(f"[YF QUOTE ERROR] {ticker}: {e}")
        return None


def yf_financials(symbol):
    ticker = symbol if "." in symbol else symbol + ".NS"
    try:
        t = yf.Ticker(ticker)
        inc = t.financials
        if inc is None or inc.empty:
            if not ticker.endswith(".BO"):
                ticker2 = symbol.replace(".NS", "") + ".BO"
                t = yf.Ticker(ticker2)
                inc = t.financials
                if inc is None or inc.empty:
                    return None
        years = []
        for col in inc.columns:
            year = str(col.year) if hasattr(col, "year") else str(col)[:4]
            rev = pat = 0
            for key in ["Total Revenue", "Operating Revenue", "Revenue"]:
                if key in inc.index:
                    val = inc.at[key, col]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        rev = float(val); break
            for key in ["Net Income", "Net Income Common Stockholders", "Net Income From Continuing Operations"]:
                if key in inc.index:
                    val = inc.at[key, col]
                    if val is not None and not (isinstance(val, float) and math.isnan(val)):
                        pat = float(val); break
            years.append({"year": year, "rev": round(rev / 1e7, 2), "pat": round(pat / 1e7, 2)})
        return years
    except Exception as e:
        log.error(f"[YF FIN ERROR] {ticker}: {e}")
        return None


def yf_search(query):
    results = []
    try:
        for suffix in [".NS", ".BO"]:
            try:
                t = yf.Ticker(query.upper() + suffix)
                info = t.info
                if info and info.get("regularMarketPrice"):
                    results.append({"sym": query.upper(), "name": info.get("longName") or query.upper(), "sec": info.get("sector") or "NSE"})
                    break
            except: continue
        try:
            sr = yf.Search(query, max_results=10)
            if hasattr(sr, 'quotes') and sr.quotes:
                for q in sr.quotes:
                    sym_raw = q.get("symbol", "")
                    if sym_raw.endswith(".NS") or sym_raw.endswith(".BO"):
                        sym_clean = sym_raw.replace(".NS", "").replace(".BO", "")
                        if not any(r["sym"] == sym_clean for r in results):
                            results.append({"sym": sym_clean, "name": q.get("longname") or sym_clean, "sec": q.get("sector") or "NSE"})
        except: pass
    except: pass
    return results[:15]


def calc_cagr(arr, field, n):
    if len(arr) < n + 1: return None
    a, b = arr[0].get(field, 0), arr[n].get(field, 0)
    if not b or b <= 0 or not a or a <= 0: return None
    return round((math.pow(a / b, 1 / n) - 1) * 100, 1)


# Stock API routes
@app.route("/api/search")
def search_stocks():
    q = request.args.get("q", "").strip()
    if not q or len(q) < 2: return jsonify([])
    ck = f"s:{q.lower()}"
    c = cached(ck, SEARCH_TTL)
    if c is not None: return jsonify(c)
    results = yf_search(q)
    set_cache(ck, results)
    return jsonify(results)


@app.route("/api/fullstock/<symbol>")
def fullstock(symbol):
    sym = symbol.upper().replace(".NS", "").replace(".BO", "")
    ck = f"f:{sym}"
    c = cached(ck, QUOTE_TTL)
    if c is not None: return jsonify(c)

    quote = yf_quote(sym)
    fck = f"fin:{sym}"
    years = cached(fck, FIN_TTL)
    if years is None:
        years = yf_financials(sym)
        if years: set_cache(fck, years)
    if not years: years = []

    cmp = quote["cmp"] if quote else 0
    mcap_cr = quote["mcap_cr"] if quote else 0
    pe = quote["pe"] if quote else 0

    result = {
        "sym": sym, "name": quote["name"] if quote else sym,
        "sec": quote["sector"] if quote else "Unknown",
        "industry": quote["industry"] if quote else "",
        "cmp": cmp, "shr": quote["shares_cr"] if quote else 0,
        "mcapCr": mcap_cr, "pe": pe, "eps": quote["eps"] if quote else 0,
        "pat": years[0]["pat"] if years else 0,
        "rev": years[0]["rev"] if years else 0,
        "r3": calc_cagr(years, "rev", 3), "r5": calc_cagr(years, "rev", 5),
        "p3": calc_cagr(years, "pat", 3), "p5": calc_cagr(years, "pat", 5),
        "dayChange": quote["change"] if quote else 0,
        "dayChangePct": quote["changePct"] if quote else 0,
        "yearHigh": quote["yearHigh"] if quote else 0,
        "yearLow": quote["yearLow"] if quote else 0,
        "bookValue": quote["bookValue"] if quote else 0,
        "dividendYield": quote["dividendYield"] if quote else 0,
        "_source": {"quote": "yfinance" if quote else "none", "financials": "yfinance" if years else "none", "years": len(years)},
    }
    # Track if logged in user
    auth = request.headers.get("Authorization", "")
    if auth.startswith("Bearer "):
        try:
            data = pyjwt.decode(auth[7:], app.config["SECRET_KEY"], algorithms=["HS256"])
            u = User.query.get(data["uid"])
            if u:
                u.stocks_analyzed = (u.stocks_analyzed or 0) + 1
                db.session.commit()
        except: pass

    set_cache(ck, result)
    return jsonify(result)


@app.route("/api/batch-quotes", methods=["POST"])
def batch_quotes():
    symbols = (request.json or {}).get("symbols", [])
    if not symbols: return jsonify([])
    results = []
    for sym in symbols[:20]:
        clean = sym.upper().replace(".NS", "").replace(".BO", "")
        q = yf_quote(clean)
        if q and q["cmp"] > 0:
            results.append({"sym": clean, "name": q["name"], "cmp": q["cmp"], "pe": q["pe"], "mcapCr": q["mcap_cr"]})
    return jsonify(results)


# ═══════ HEALTH & TEST ═══════

@app.route("/")
def health():
    return jsonify({
        "status": "ok", "service": "DIY Investing API v6",
        "source": "yfinance (INR native)",
        "features": ["auth", "google_oauth", "razorpay", "watchlist", "admin"],
        "db": "connected" if db.engine else "error",
    })


@app.route("/api/test")
def test_api():
    result = {"status": "ok", "tests": {}}
    try:
        q = yf_quote("TCS")
        result["tests"]["quote"] = {"working": bool(q and q["cmp"] > 0), "tcs_cmp": q["cmp"] if q else 0}
    except Exception as e:
        result["tests"]["quote"] = {"working": False, "error": str(e)}
    try:
        years = yf_financials("TCS")
        result["tests"]["financials"] = {"working": bool(years), "years": len(years) if years else 0}
    except Exception as e:
        result["tests"]["financials"] = {"working": False, "error": str(e)}
    try:
        count = User.query.count()
        result["tests"]["database"] = {"working": True, "users": count}
    except Exception as e:
        result["tests"]["database"] = {"working": False, "error": str(e)}
    return jsonify(result)


# ═══════ DB INIT & START ═══════

with app.app_context():
    db.create_all()
    log.info("Database tables created/verified")


if __name__ == "__main__":
    log.info(f"\n{'='*50}")
    log.info(f"  DIY Investing API v6")
    log.info(f"  Port: {PORT}")
    log.info(f"  DB: {app.config['SQLALCHEMY_DATABASE_URI'][:40]}...")
    log.info(f"  Google OAuth: {'YES' if GOOGLE_CLIENT_ID else 'NO'}")
    log.info(f"  Razorpay: {'YES' if RAZORPAY_KEY_ID else 'NO'}")
    log.info(f"  Admin emails: {ADMIN_EMAILS}")
    log.info(f"{'='*50}\n")
    app.run(host="0.0.0.0", port=PORT, debug=False)
