package services

import "testing"

func strp(s string) *string { return &s }

func deref(p *string) string {
	if p == nil {
		return "<nil>"
	}
	return *p
}

func TestParseProxyLine(t *testing.T) {
	tests := []struct {
		name string
		line string
		ok   bool
		want parsedProxy
	}{
		// ── existing formats (regression) ──────────────────────────
		{
			name: "bare host:port (IPv4)",
			line: "1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{address: "1.2.3.4:8080"},
		},
		{
			name: "bare host:port (hostname)",
			line: "proxy.example.com:3128",
			ok:   true,
			want: parsedProxy{address: "proxy.example.com:3128"},
		},
		{
			name: "userinfo@host:port",
			line: "bob:secret@1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{
				address:  "1.2.3.4:8080",
				username: strp("bob"),
				password: strp("secret"),
			},
		},
		{
			name: "scheme + host:port",
			line: "http://1.2.3.4:8080",
			ok:   true,
			want: parsedProxy{address: "1.2.3.4:8080", protocol: "http"},
		},
		{
			name: "scheme + userinfo + host:port",
			line: "socks5://alice:p4ss@host.example.com:1080",
			ok:   true,
			want: parsedProxy{
				address:  "host.example.com:1080",
				protocol: "socks5",
				username: strp("alice"),
				password: strp("p4ss"),
			},
		},

		// ── NEW: host:port:user:pass ───────────────────────────────
		{
			name: "colon-separated creds (IPv4)",
			line: "10.1.2.4:6511:username:password",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				username: strp("username"),
				password: strp("password"),
			},
		},
		{
			name: "colon-separated creds (hostname)",
			line: "proxy.example.com:3128:bob:secret",
			ok:   true,
			want: parsedProxy{
				address:  "proxy.example.com:3128",
				username: strp("bob"),
				password: strp("secret"),
			},
		},
		{
			name: "colon-separated creds with ':' in password",
			line: "10.1.2.4:6511:bob:pa:ss:word",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				username: strp("bob"),
				password: strp("pa:ss:word"),
			},
		},
		{
			name: "scheme + colon-separated creds",
			line: "http://10.1.2.4:6511:bob:secret",
			ok:   true,
			want: parsedProxy{
				address:  "10.1.2.4:6511",
				protocol: "http",
				username: strp("bob"),
				password: strp("secret"),
			},
		},

		// ── rejections / fallthrough ───────────────────────────────
		{name: "empty", line: "", ok: false},
		{name: "comment", line: "# 1.2.3.4:8080", ok: false},
		{name: "host without port", line: "hostonly", ok: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := parseProxyLine(tt.line)
			if ok != tt.ok {
				t.Fatalf("ok = %v, want %v (got=%+v)", ok, tt.ok, got)
			}
			if !ok {
				return
			}
			if got.address != tt.want.address {
				t.Errorf("address = %q, want %q", got.address, tt.want.address)
			}
			if got.protocol != tt.want.protocol {
				t.Errorf("protocol = %q, want %q", got.protocol, tt.want.protocol)
			}
			if deref(got.username) != deref(tt.want.username) {
				t.Errorf("username = %s, want %s", deref(got.username), deref(tt.want.username))
			}
			if deref(got.password) != deref(tt.want.password) {
				t.Errorf("password = %s, want %s", deref(got.password), deref(tt.want.password))
			}
		})
	}
}
