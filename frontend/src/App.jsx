import { useEffect, useMemo, useState } from "react";
import {
  CalendarDays,
  CheckCircle2,
  CircleDollarSign,
  Hotel,
  Loader2,
  MapPin,
  MessageSquareText,
  RefreshCw,
  Send,
  Sparkles,
  Users
} from "lucide-react";

const API_BASE = import.meta.env.VITE_API_BASE || "/api";

const roomImages = {
  101: "https://images.unsplash.com/photo-1566073771259-6a8506099945?auto=format&fit=crop&w=1200&q=80",
  102: "https://images.unsplash.com/photo-1590490360182-c33d57733427?auto=format&fit=crop&w=1200&q=80",
  201: "https://images.unsplash.com/photo-1578683010236-d716f9a3f461?auto=format&fit=crop&w=1200&q=80",
  301: "https://images.unsplash.com/photo-1542314831-068cd1dbfeeb?auto=format&fit=crop&w=1200&q=80",
  302: "https://images.unsplash.com/photo-1611892440504-42a792e24d32?auto=format&fit=crop&w=1200&q=80",
  401: "https://images.unsplash.com/photo-1582719508461-905c673771fd?auto=format&fit=crop&w=1200&q=80"
};

const fallbackImage =
  "https://images.unsplash.com/photo-1500530855697-b586d89ba3ee?auto=format&fit=crop&w=1200&q=80";

const experiences = [
  {
    title: "云海晨间",
    kicker: "清晨 05:30",
    text: "沿山顶步道抵达观景平台，把早餐和日出留给第一束光。",
    image: "https://images.unsplash.com/photo-1500534314209-a25ddb2bd429?auto=format&fit=crop&w=1100&q=80"
  },
  {
    title: "森林温泉",
    kicker: "预约制",
    text: "套房住客可预约半露天汤池，结束一天行程后慢慢放松。",
    image: "https://images.unsplash.com/photo-1540541338287-41700207dee6?auto=format&fit=crop&w=1100&q=80"
  },
  {
    title: "湖畔晚餐",
    kicker: "18:00-21:30",
    text: "餐厅供应季节菜单，适合家庭、情侣和商务小队。",
    image: "https://images.unsplash.com/photo-1555396273-367ea4eb4db5?auto=format&fit=crop&w=1100&q=80"
  }
];

const userOptions = [
  { id: 1, label: "Alice · GOLD" },
  { id: 2, label: "Bob · SILVER" }
];

function addDays(date, days) {
  const next = new Date(date);
  next.setDate(next.getDate() + days);
  return next.toISOString().slice(0, 10);
}

async function request(path, options) {
  const response = await fetch(`${API_BASE}${path}`, {
    headers: {
      "Content-Type": "application/json",
      ...(options?.headers || {})
    },
    ...options
  });
  const contentType = response.headers.get("content-type") || "";
  const payload = contentType.includes("application/json") ? await response.json() : await response.text();
  if (!response.ok) {
    throw new Error(typeof payload === "string" ? payload : payload.message || `HTTP ${response.status}`);
  }
  if (payload && typeof payload === "object" && "success" in payload) {
    if (!payload.success) {
      throw new Error(payload.message || "请求失败");
    }
    return payload.data;
  }
  return payload;
}

export default function App() {
  const today = useMemo(() => new Date().toISOString().slice(0, 10), []);
  const [rooms, setRooms] = useState([]);
  const [orders, setOrders] = useState([]);
  const [events, setEvents] = useState({ rabbitmq: [], kafka: [] });
  const [selectedRoomId, setSelectedRoomId] = useState(101);
  const [form, setForm] = useState({
    userId: 1,
    checkIn: addDays(new Date(), 1),
    checkOut: addDays(new Date(), 3)
  });
  const [loading, setLoading] = useState(true);
  const [booking, setBooking] = useState(false);
  const [notice, setNotice] = useState("");
  const [error, setError] = useState("");

  const selectedRoom = rooms.find((room) => room.id === Number(selectedRoomId)) || rooms[0];
  const nights = Math.max(
    1,
    Math.round((new Date(form.checkOut).getTime() - new Date(form.checkIn).getTime()) / 86400000)
  );
  const totalAmount = selectedRoom ? Number(selectedRoom.price) * nights : 0;
  const eventCount = (events.rabbitmq?.length || 0) + (events.kafka?.length || 0);

  useEffect(() => {
    refreshAll();
  }, []);

  async function refreshAll() {
    setLoading(true);
    setError("");
    try {
      const [roomData, orderData, eventData] = await Promise.all([
        request("/hotels"),
        request("/orders").catch(() => []),
        request("/messages/booking-events").catch(() => ({ rabbitmq: [], kafka: [] }))
      ]);
      setRooms(roomData || []);
      setOrders(Array.isArray(orderData) ? orderData : []);
      setEvents(eventData || { rabbitmq: [], kafka: [] });
      if (roomData?.length && !roomData.some((room) => room.id === Number(selectedRoomId))) {
        setSelectedRoomId(roomData[0].id);
      }
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function submitBooking(event) {
    event.preventDefault();
    if (!selectedRoom) {
      setError("请先选择房型");
      return;
    }
    setBooking(true);
    setError("");
    setNotice("");
    try {
      const order = await request("/orders/book", {
        method: "POST",
        body: JSON.stringify({
          userId: Number(form.userId),
          roomId: Number(selectedRoom.id),
          checkIn: form.checkIn,
          checkOut: form.checkOut
        })
      });
      setNotice(`预订成功：${order.orderId}，支付状态 ${order.status}`);
      await refreshAll();
    } catch (err) {
      setError(err.message);
    } finally {
      setBooking(false);
    }
  }

  return (
    <main>
      <header className="site-nav">
        <a className="brand" href="#top" aria-label="云境山谷度假酒店">
          <span>云境山谷</span>
          <small>Resort Booking</small>
        </a>
        <nav aria-label="主要导航">
          <a href="#rooms">房型</a>
          <a href="#experience">体验</a>
          <a href="#orders">订单</a>
        </nav>
      </header>

      <section id="top" className="hero">
        <div className="hero-copy">
          <span className="eyebrow">Green Season 2026</span>
          <h1>云境山谷度假酒店</h1>
          <p>把酒店预订、库存扣减、支付模拟和消息通知串成一个可体验的完整链路。</p>
        </div>

        <form className="booking-bar" onSubmit={submitBooking}>
          <label>
            <span><Users size={16} />住客</span>
            <select
              value={form.userId}
              onChange={(event) => setForm((current) => ({ ...current, userId: event.target.value }))}
            >
              {userOptions.map((user) => (
                <option key={user.id} value={user.id}>{user.label}</option>
              ))}
            </select>
          </label>
          <label>
            <span><CalendarDays size={16} />入住</span>
            <input
              type="date"
              min={today}
              value={form.checkIn}
              onChange={(event) => setForm((current) => ({ ...current, checkIn: event.target.value }))}
            />
          </label>
          <label>
            <span><CalendarDays size={16} />离店</span>
            <input
              type="date"
              min={form.checkIn}
              value={form.checkOut}
              onChange={(event) => setForm((current) => ({ ...current, checkOut: event.target.value }))}
            />
          </label>
          <label>
            <span><Hotel size={16} />房型</span>
            <select
              value={selectedRoomId}
              onChange={(event) => setSelectedRoomId(event.target.value)}
            >
              {rooms.map((room) => (
                <option key={room.id} value={room.id}>
                  {room.hotelName} · {room.roomType}
                </option>
              ))}
            </select>
          </label>
          <button className="primary-button" type="submit" disabled={booking || loading || !selectedRoom}>
            {booking ? <Loader2 className="spin" size={18} /> : <Send size={18} />}
            立即预订
          </button>
        </form>
      </section>

      {(notice || error) && (
        <section className={`toast ${error ? "is-error" : ""}`} aria-live="polite">
          {error || notice}
        </section>
      )}

      <section className="summary-strip" aria-label="业务概览">
        <div>
          <strong>{rooms.length}</strong>
          <span>在线房型</span>
        </div>
        <div>
          <strong>{orders.length}</strong>
          <span>已生成订单</span>
        </div>
        <div>
          <strong>{eventCount}</strong>
          <span>已消费消息</span>
        </div>
        <div>
          <strong>{selectedRoom ? `¥${totalAmount.toFixed(0)}` : "¥0"}</strong>
          <span>{nights} 晚预估</span>
        </div>
      </section>

      <section id="rooms" className="section">
        <div className="section-heading">
          <span className="eyebrow">Hotels</span>
          <h2>选择你的住宿</h2>
          <button className="ghost-button" type="button" onClick={refreshAll} disabled={loading}>
            {loading ? <Loader2 className="spin" size={17} /> : <RefreshCw size={17} />}
            刷新
          </button>
        </div>

        <div className="room-grid">
          {rooms.map((room) => (
            <article
              className={`room-card ${room.id === selectedRoom?.id ? "is-selected" : ""}`}
              key={room.id}
            >
              <img src={roomImages[room.id] || fallbackImage} alt={`${room.hotelName} ${room.roomType}`} />
              <div className="room-body">
                <div>
                  <span className="room-id">#{room.id}</span>
                  <h3>{room.hotelName}</h3>
                  <p>{room.roomType}</p>
                </div>
                <dl>
                  <div>
                    <dt>每晚</dt>
                    <dd>¥{Number(room.price).toFixed(0)}</dd>
                  </div>
                  <div>
                    <dt>余量</dt>
                    <dd>{room.available}</dd>
                  </div>
                </dl>
                <button type="button" onClick={() => setSelectedRoomId(room.id)} disabled={room.available <= 0}>
                  <CheckCircle2 size={18} />
                  {room.available > 0 ? "选择房型" : "暂满"}
                </button>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section id="experience" className="section muted-section">
        <div className="section-heading">
          <span className="eyebrow">Experience</span>
          <h2>度假体验</h2>
        </div>
        <div className="experience-grid">
          {experiences.map((item) => (
            <article className="experience-card" key={item.title}>
              <img src={item.image} alt={item.title} />
              <div>
                <span>{item.kicker}</span>
                <h3>{item.title}</h3>
                <p>{item.text}</p>
              </div>
            </article>
          ))}
        </div>
      </section>

      <section id="orders" className="section dashboard-section">
        <div className="section-heading">
          <span className="eyebrow">Booking Flow</span>
          <h2>订单与消息流</h2>
        </div>

        <div className="flow-grid">
          <article className="flow-panel">
            <div className="panel-title">
              <CircleDollarSign size={20} />
              <h3>最近订单</h3>
            </div>
            <div className="list">
              {orders.length === 0 && <p className="empty">还没有订单，先完成一次预订。</p>}
              {orders.slice().reverse().map((order) => (
                <div className="list-row" key={order.orderId}>
                  <span>{order.orderId}</span>
                  <strong>{order.status}</strong>
                  <small>房型 {order.roomId} · ¥{Number(order.amount).toFixed(0)}</small>
                </div>
              ))}
            </div>
          </article>

          <article className="flow-panel">
            <div className="panel-title">
              <MessageSquareText size={20} />
              <h3>RabbitMQ / Kafka</h3>
            </div>
            <div className="message-columns">
              <MessageList title="RabbitMQ" items={events.rabbitmq || []} />
              <MessageList title="Kafka" items={events.kafka || []} />
            </div>
          </article>
        </div>
      </section>

      <footer>
        <span><MapPin size={16} />Demo Resort</span>
        <span><Sparkles size={16} />Spring Cloud Alibaba · React</span>
      </footer>
    </main>
  );
}

function MessageList({ title, items }) {
  return (
    <div className="message-list">
      <h4>{title}</h4>
      {items.length === 0 && <p className="empty">暂无消息</p>}
      {items.slice(-4).reverse().map((item, index) => (
        <div className="message-row" key={`${title}-${item.orderId}-${index}`}>
          <strong>{item.orderId}</strong>
          <span>房型 {item.roomId} · 用户 {item.userId}</span>
        </div>
      ))}
    </div>
  );
}
