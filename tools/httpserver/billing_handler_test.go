package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/sharedcode/joltrin/v5/governance"
)

// TestMain isolates config.DatabasePath to a fresh temp directory before any
// test runs. getBillingService() is a package-level sync.Once singleton
// that persists billing state under config.DatabasePath; without this,
// config.DatabasePath is "" during `go test` (main()'s flag.Parse never
// runs), so the singleton would fall back to the fixed "/tmp/sop_data"
// default and write real files there instead of a sandboxed temp dir.
//
// This alone doesn't fully sandbox billing state: many test files in this
// package do a blanket `config = Config{}` reset for their own isolation
// needs, without preserving DatabasePath, and getBillingService()'s
// sync.Once only cares about whatever config.DatabasePath happens to be at
// the moment something *first* calls it. Depending on execution order that
// can still land on the /tmp/sop_data fallback (confirmed locally: this is
// exactly what caused TestHandleSimulateCheckout to intermittently fail
// with a stale "duplicate webhook event" error days after it was last
// run - /tmp/sop_data/_billing had accumulated real state across separate
// `go test` invocations on the same machine). Forcing getBillingService()
// to initialize eagerly right here was tried and made things worse: it
// then reliably captured the FeatureGate that exists at TestMain time,
// before auth_oidc_test.go's setupTestOIDC deliberately does
// `oidcOnce = sync.Once{}` for its own isolation - which replaces the
// package-level serverFeatureGate with a new instance that
// getBillingService()'s already-captured gate reference has no way to
// find out about. Two singletons that are each individually correct for
// their own test file become permanently out of sync with each other
// depending on which one initializes first; fixing that for real means
// DefaultBillingService not caching a gate reference across its own
// lifetime, which is a production code change out of scope here.
//
// So this only closes the DatabasePath gap, not gate staleness: it does
// not eagerly force initialization, and any test asserting on
// getServerFeatureGate() state after a billing webhook needs its own
// event/session ID to be unique (see TestHandleSimulateCheckout and
// siblings) rather than relying on this to guarantee a clean slate.
func TestMain(m *testing.M) {
	tmpDir, err := os.MkdirTemp("", "joltrin-httpserver-test-*")
	if err != nil {
		panic(err)
	}
	config.DatabasePath = tmpDir

	code := m.Run()
	os.RemoveAll(tmpDir)
	os.Exit(code)
}

func TestHandleGetPlan(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/api/billing/plan", nil)
	w := httptest.NewRecorder()

	handleGetPlan(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON response: %v", err)
	}

	if _, ok := resp["current_tier"]; !ok {
		t.Error("expected 'current_tier' field in response")
	}
	if _, ok := resp["capabilities"]; !ok {
		t.Error("expected 'capabilities' field in response")
	}
	if _, ok := resp["subscription"]; !ok {
		t.Error("expected 'subscription' field in response")
	}
}

func TestHandleCreateCheckoutSession(t *testing.T) {
	reqBody := map[string]any{
		"tier":      "pro",
		"tenant_id": "test-tenant-1",
		"email":     "admin@example.com",
	}
	bodyBytes, _ := json.Marshal(reqBody)
	req := httptest.NewRequest(http.MethodPost, "/api/billing/checkout", bytes.NewReader(bodyBytes))
	w := httptest.NewRecorder()

	handleCreateCheckoutSession(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}

	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
	if resp["checkout_url"] == "" {
		t.Error("expected non-empty checkout_url")
	}
}

func TestHandlePublicCheckout_CORS(t *testing.T) {
	pre := httptest.NewRequest(http.MethodOptions, "/api/billing/public-checkout", nil)
	pre.Header.Set("Origin", "https://joltrinhq.com")
	w := httptest.NewRecorder()
	handlePublicCheckout(w, pre)
	if w.Code != http.StatusNoContent {
		t.Fatalf("preflight: expected 204, got %d", w.Code)
	}
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "https://joltrinhq.com" {
		t.Errorf("expected joltrinhq.com allowed, got %q", got)
	}

	other := httptest.NewRequest(http.MethodOptions, "/api/billing/public-checkout", nil)
	other.Header.Set("Origin", "https://evil.example")
	w = httptest.NewRecorder()
	handlePublicCheckout(w, other)
	if got := w.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("unexpected CORS header for foreign origin: %q", got)
	}

}

func TestHandlePublicCheckout_Requests(t *testing.T) {
	post := func(body string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/api/billing/public-checkout", strings.NewReader(body))
		req.Header.Set("Origin", "https://joltrinhq.com")
		w := httptest.NewRecorder()
		handlePublicCheckout(w, req)
		return w
	}

	if w := post(`{"email":"not-an-email"}`); w.Code != http.StatusBadRequest {
		t.Errorf("bad email: expected 400, got %d", w.Code)
	}
	if w := post(`{`); w.Code != http.StatusBadRequest {
		t.Errorf("bad json: expected 400, got %d", w.Code)
	}

	get := httptest.NewRecorder()
	handlePublicCheckout(get, httptest.NewRequest(http.MethodGet, "/api/billing/public-checkout", nil))
	if get.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: expected 405, got %d", get.Code)
	}

	// Tests run with no Stripe keys, so billing is simulated. The public
	// route must refuse rather than hand out a simulated session URL that
	// grants a plan without payment, and a caller cannot pick tier or tenant.
	w := post(`{"email":"a@example.com","tier":"enterprise","tenant_id":"victim"}`)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("simulation mode: expected 503, got %d: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "checkout_url") {
		t.Errorf("simulated checkout URL leaked: %s", w.Body.String())
	}
}

func TestHandleSimulateCheckout(t *testing.T) {
	gate := getServerFeatureGate()
	gate.SetTier(governance.TierCore)

	// The webhook event ID must be unique per test run, not a hardcoded
	// literal: getBillingService() treats it as an idempotency key and
	// rejects a repeat as a duplicate, which previously made this test
	// depend on never having run before against whatever storage path the
	// billing service singleton happened to land on (see TestMain).
	sessionID := fmt.Sprintf("cs_sim_%d", time.Now().UnixNano())
	req := httptest.NewRequest(http.MethodGet, "/api/billing/checkout/simulate?session_id="+sessionID+"&tenant_id=tenant-sim&tier=pro&redirect=/app", nil)
	w := httptest.NewRecorder()

	handleSimulateCheckout(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect, got %d: %s", w.Code, w.Body.String())
	}

	if loc := w.Header().Get("Location"); loc != "/app" {
		t.Errorf("expected redirect to /app, got %s", loc)
	}

	// Verify server FeatureGate was upgraded
	if gate.Tier() != governance.TierPro {
		t.Errorf("expected gate to be upgraded to TierPro, got %s", gate.Tier())
	}
}

func TestHandleSimulateCheckout_RejectsOpenRedirect(t *testing.T) {
	gate := getServerFeatureGate()
	gate.SetTier(governance.TierCore)

	sessionID := fmt.Sprintf("cs_sim_evil_%d", time.Now().UnixNano())
	req := httptest.NewRequest(http.MethodGet, "/api/billing/checkout/simulate?session_id="+sessionID+"&tenant_id=tenant-sim&tier=pro&redirect=https://evil.example/phish", nil)
	w := httptest.NewRecorder()

	handleSimulateCheckout(w, req)

	if w.Code != http.StatusFound {
		t.Fatalf("expected 302 redirect, got %d: %s", w.Code, w.Body.String())
	}
	if loc := w.Header().Get("Location"); loc == "https://evil.example/phish" {
		t.Fatalf("attacker-controlled redirect target was honored: %s", loc)
	}
}

// TestHandleSimulateCheckout_BypassAttemptsStaySameOrigin is the end-to-end
// half of the CodeQL alert 268 investigation: it drives the real handler
// with a "redirect" query parameter built the way an actual attacker
// request would carry one (via url.Values, so encoding matches the wire
// format exactly) for every bypass class considered, and asserts the
// resulting Location header, when resolved against the site's own origin,
// never ends up pointing at a different host. This is the property that
// actually matters; isSafeRelativeRedirect's own accept/reject choice for
// each payload is exercised separately in redirect_safety_test.go.
func TestHandleSimulateCheckout_BypassAttemptsStaySameOrigin(t *testing.T) {
	gate := getServerFeatureGate()
	gate.SetTier(governance.TierCore)

	payloads := []string{
		"https://evil.example/phish",
		"http://evil.example/phish",
		"//evil.example/phish",
		"///evil.example/phish",
		`/\evil.example/phish`,
		`/\\evil.example/phish`,
		"/%2F%2Fevil.example",
		"/%5Cevil.example",
		"/%252F%252Fevil.example",
		"/\t/evil.example",
		"/\n/evil.example",
		"/\r/evil.example",
		"/\r\nSet-Cookie: pwn=1",
		"/ /evil.example",
		" //evil.example",
		"/../../evil.example",
		"/%2e%2e/evil.example",
		"/@evil.example",
		"/HTTP://evil.example",
		"/JAVASCRIPT:alert(1)",
		"/／evil.example",
	}

	origin := &url.URL{Scheme: "https", Host: "joltrin.example"}
	runID := time.Now().UnixNano()

	for _, redirect := range payloads {
		t.Run(redirect, func(t *testing.T) {
			q := url.Values{}
			// Unique per test run (not per payload - the point here is the
			// redirect target, and a repeat event ID is fine since
			// getBillingService() treats it as an already-handled
			// duplicate rather than an error).
			q.Set("session_id", fmt.Sprintf("cs_sim_bypass_%d", runID))
			q.Set("tenant_id", "tenant-sim")
			q.Set("tier", "pro")
			q.Set("redirect", redirect)

			req := httptest.NewRequest(http.MethodGet, "/api/billing/checkout/simulate?"+q.Encode(), nil)
			w := httptest.NewRecorder()

			handleSimulateCheckout(w, req)

			loc := w.Header().Get("Location")
			if loc == "" {
				t.Fatalf("no Location header set for redirect=%q", redirect)
			}
			resolved, err := origin.Parse(loc)
			if err != nil {
				t.Fatalf("Location header %q from redirect=%q did not parse: %v", loc, redirect, err)
			}
			if resolved.Host != origin.Host {
				t.Errorf("redirect=%q produced off-origin Location %q (resolved host %q, want %q)",
					redirect, loc, resolved.Host, origin.Host)
			}
		})
	}
}

func TestHandleSubmitEnterpriseInquiry(t *testing.T) {
	inq := map[string]any{
		"name":      "VP of Engineering",
		"email":     "vp@enterprise-ai.com",
		"company":   "Enterprise AI Systems",
		"team_size": "50-200",
		"use_cases": "Federated MCP memory with Okta SSO",
	}
	body, _ := json.Marshal(inq)
	req := httptest.NewRequest(http.MethodPost, "/api/billing/enterprise-contact", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handleSubmitEnterpriseInquiry(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding JSON: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
}

func TestHandleBillingWebhook_ValidSignatureAndUpgrade(t *testing.T) {
	secret := "whsec_handler_test_secret"
	svc := getBillingService().(*governance.DefaultBillingService)
	// Temporarily set webhook secret on the service for the test
	testSvc := governance.NewDefaultBillingService(governance.StripeConfig{
		WebhookSecret: secret,
	}, getServerFeatureGate())

	// Swap billing service for this test
	oldSvc := billingService
	billingService = testSvc
	defer func() { billingService = oldSvc }()

	payload := governance.ConstructSimulatedWebhookPayload("checkout.session.completed", "evt_wh_handler_1", "tenant-webhook", governance.TierPro)
	sigHeader := governance.GenerateStripeSignatureHeader(payload, secret, time.Now())

	req := httptest.NewRequest(http.MethodPost, "/api/billing/webhook", bytes.NewReader(payload))
	req.Header.Set("Stripe-Signature", sigHeader)
	w := httptest.NewRecorder()

	handleBillingWebhook(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("failed decoding response: %v", err)
	}
	if resp["status"] != "ok" {
		t.Errorf("expected status ok, got %v", resp["status"])
	}
	_ = svc
}

func TestHandleBillingWebhook_InvalidSignature_Rejected(t *testing.T) {
	secret := "whsec_handler_test_secret"
	testSvc := governance.NewDefaultBillingService(governance.StripeConfig{
		WebhookSecret: secret,
	}, getServerFeatureGate())

	oldSvc := billingService
	billingService = testSvc
	defer func() { billingService = oldSvc }()

	payload := []byte(`{"id":"evt_attack","type":"checkout.session.completed"}`)
	// Invalid signature header
	sigHeader := "t=1492774577,v1=badbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbadbad"

	req := httptest.NewRequest(http.MethodPost, "/api/billing/webhook", bytes.NewReader(payload))
	req.Header.Set("Stripe-Signature", sigHeader)
	w := httptest.NewRecorder()

	handleBillingWebhook(w, req)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400 Bad Request for bad signature, got %d", w.Code)
	}
}

func TestHandleBillingWebhook_DuplicateIgnoredGracefully(t *testing.T) {
	secret := "whsec_duplicate_test"
	testSvc := governance.NewDefaultBillingService(governance.StripeConfig{
		WebhookSecret: secret,
	}, getServerFeatureGate())

	oldSvc := billingService
	billingService = testSvc
	defer func() { billingService = oldSvc }()

	payload := governance.ConstructSimulatedWebhookPayload("checkout.session.completed", "evt_duplicate_wh", "tenant-dup2", governance.TierPro)
	sigHeader := governance.GenerateStripeSignatureHeader(payload, secret, time.Now())

	// 1st request -> 200 OK
	req1 := httptest.NewRequest(http.MethodPost, "/api/billing/webhook", bytes.NewReader(payload))
	req1.Header.Set("Stripe-Signature", sigHeader)
	w1 := httptest.NewRecorder()
	handleBillingWebhook(w1, req1)
	if w1.Code != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", w1.Code)
	}

	// 2nd request with same event ID -> 200 OK with ignored_duplicate status
	req2 := httptest.NewRequest(http.MethodPost, "/api/billing/webhook", bytes.NewReader(payload))
	req2.Header.Set("Stripe-Signature", sigHeader)
	w2 := httptest.NewRecorder()
	handleBillingWebhook(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("expected 200 OK for duplicate event to prevent Stripe retry storm, got %d", w2.Code)
	}

	var resp map[string]any
	_ = json.Unmarshal(w2.Body.Bytes(), &resp)
	if resp["status"] != "ignored_duplicate" {
		t.Errorf("expected status 'ignored_duplicate', got %v", resp["status"])
	}
}

func envFrom(m map[string]string) func(string) string {
	return func(k string) string { return m[k] }
}

func TestLoadStripeConfig_NoKeysMeansSimulation(t *testing.T) {
	cfg := loadStripeConfig(envFrom(nil))
	if !cfg.Simulate || cfg.SecretKey != "" {
		t.Fatalf("expected simulation with no keys, got %+v", cfg)
	}
	if r := governance.AssessBilling(cfg); r.Mode != "simulated" || r.Enterprise.Mode != "contact_sales" {
		t.Fatalf("unexpected readiness %+v", r)
	}
}

func TestLoadStripeConfig_LiveKeyWithoutPriceOrURLsIsNotReady(t *testing.T) {
	cfg := loadStripeConfig(envFrom(map[string]string{"STRIPE_SECRET_KEY": "sk_test_placeholder"}))
	r := governance.AssessBilling(cfg)
	if r.Mode != "live" || r.Pro.Available {
		t.Fatalf("live key alone must not enable checkout: %+v", r)
	}
}

func TestLoadStripeConfig_PublicURLBuildsAbsoluteReturnURLs(t *testing.T) {
	cfg := loadStripeConfig(envFrom(map[string]string{
		"STRIPE_SECRET_KEY":     "sk_test_placeholder",
		"STRIPE_WEBHOOK_SECRET": "whsec_placeholder",
		"STRIPE_PRO_PRICE_ID":   "price_pro_real",
		"JOLTRIN_PUBLIC_URL":    "https://app.example.test/",
	}))
	if cfg.SuccessURL != "https://app.example.test/app?checkout=success&session_id={CHECKOUT_SESSION_ID}" {
		t.Errorf("unexpected success URL %q", cfg.SuccessURL)
	}
	if r := governance.AssessBilling(cfg); !r.Pro.Available || r.Enterprise.Mode != "contact_sales" {
		t.Fatalf("expected Pro ready and Enterprise contact-sales, got %+v", r)
	}
}

func withBillingService(t *testing.T, cfg governance.StripeConfig) {
	t.Helper()
	old := billingService
	billingService = governance.NewDefaultBillingService(cfg, getServerFeatureGate())
	t.Cleanup(func() { billingService = old })
}

func TestHandleGetPlan_ReportsCheckoutReadiness(t *testing.T) {
	withBillingService(t, governance.StripeConfig{SecretKey: "sk_test_placeholder"})

	w := httptest.NewRecorder()
	handleGetPlan(w, httptest.NewRequest(http.MethodGet, "/api/billing/plan", nil))

	var resp struct {
		Checkout governance.BillingReadiness `json:"checkout"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Checkout.Mode != "live" || resp.Checkout.Pro.Available || len(resp.Checkout.Pro.Missing) == 0 {
		t.Fatalf("expected live but not ready, got %+v", resp.Checkout)
	}
	if bytes.Contains(w.Body.Bytes(), []byte("sk_test_placeholder")) {
		t.Fatal("plan response leaked the secret key")
	}
}

func TestHandleCreateCheckoutSession_EnterpriseIsContactSales(t *testing.T) {
	withBillingService(t, governance.StripeConfig{Simulate: true})

	body, _ := json.Marshal(map[string]any{"tier": "enterprise", "email": "a@example.com"})
	w := httptest.NewRecorder()
	handleCreateCheckoutSession(w, httptest.NewRequest(http.MethodPost, "/api/billing/checkout", bytes.NewReader(body)))
	if w.Code != http.StatusConflict {
		t.Fatalf("expected 409, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleCreateCheckoutSession_LiveNotConfiguredIs503(t *testing.T) {
	withBillingService(t, governance.StripeConfig{SecretKey: "sk_test_placeholder"})

	body, _ := json.Marshal(map[string]any{"tier": "pro", "email": "a@example.com"})
	w := httptest.NewRecorder()
	handleCreateCheckoutSession(w, httptest.NewRequest(http.MethodPost, "/api/billing/checkout", bytes.NewReader(body)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandleSimulateCheckout_NotFoundInLiveMode(t *testing.T) {
	gate := getServerFeatureGate()
	gate.SetTier(governance.TierCore)
	withBillingService(t, governance.StripeConfig{SecretKey: "sk_test_placeholder", WebhookSecret: "whsec_placeholder"})

	w := httptest.NewRecorder()
	handleSimulateCheckout(w, httptest.NewRequest(http.MethodGet, "/api/billing/checkout/simulate?tier=pro", nil))
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
	if gate.Tier() != governance.TierCore {
		t.Fatalf("simulate route must not change the tier in live mode, got %s", gate.Tier())
	}
}

func TestHandleBillingWebhook_RefusedWithoutSigningSecret(t *testing.T) {
	gate := getServerFeatureGate()
	gate.SetTier(governance.TierCore)
	withBillingService(t, governance.StripeConfig{SecretKey: "sk_test_placeholder"})

	payload := governance.ConstructSimulatedWebhookPayload("checkout.session.completed", "evt_no_secret_handler", "tenant-x", governance.TierPro)
	w := httptest.NewRecorder()
	handleBillingWebhook(w, httptest.NewRequest(http.MethodPost, "/api/billing/webhook", bytes.NewReader(payload)))
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected 503, got %d", w.Code)
	}
	if gate.Tier() != governance.TierCore {
		t.Fatalf("unsigned event must not change the tier, got %s", gate.Tier())
	}
}

func TestLoadStripeConfig_UnsetPlaceholderStaysInSimulation(t *testing.T) {
	cfg := loadStripeConfig(envFrom(map[string]string{
		"STRIPE_SECRET_KEY":      "unset",
		"STRIPE_WEBHOOK_SECRET":  "unset",
		"STRIPE_PUBLISHABLE_KEY": "unset",
	}))
	if !cfg.Simulate || cfg.SecretKey != "" || cfg.WebhookSecret != "" || cfg.PublishableKey != "" {
		t.Fatalf("the Key Vault placeholder must be treated as empty, got %+v", cfg)
	}
}

func TestLoadStripeConfig_ManagedPayments(t *testing.T) {
	if loadStripeConfig(envFrom(nil)).ManagedPayments {
		t.Fatal("managed payments must be off by default")
	}
	cfg := loadStripeConfig(envFrom(map[string]string{"STRIPE_MANAGED_PAYMENTS": "true"}))
	if !cfg.ManagedPayments {
		t.Fatal("STRIPE_MANAGED_PAYMENTS=true should enable managed payments")
	}
}

var inquiryClient int32

func postInquiry(t *testing.T, origin string, body []byte) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/api/billing/enterprise-contact", bytes.NewReader(body))
	req.RemoteAddr = "203.0.113.7:4000"
	// A distinct client address per call, so tests do not share a rate limit bucket.
	req.Header.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", atomic.AddInt32(&inquiryClient, 1)))
	if origin != "" {
		req.Header.Set("Origin", origin)
	}
	w := httptest.NewRecorder()
	handleSubmitEnterpriseInquiry(w, req)
	return w
}

func TestInquiryRejectsOversizedBodies(t *testing.T) {
	body, _ := json.Marshal(map[string]string{"email": "a@example.com", "message": strings.Repeat("x", 20<<10)})
	if w := postInquiry(t, "", body); w.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("a 20 KiB body: got %d, want 413", w.Code)
	}
}

func TestInquiryRejectsBadInput(t *testing.T) {
	for name, body := range map[string]string{
		"not json":      `nope`,
		"no email":      `{"name":"x"}`,
		"invalid email": `{"email":"nope"}`,
		"long field":    `{"email":"a@example.com","name":"` + strings.Repeat("x", 300) + `"}`,
	} {
		if w := postInquiry(t, "", []byte(body)); w.Code != http.StatusBadRequest {
			t.Errorf("%s: got %d, want 400", name, w.Code)
		}
	}
}

func TestInquiryIgnoresACallerChosenIDAndDoesNotEchoTheInquiry(t *testing.T) {
	w := postInquiry(t, "", []byte(`{"email":"a@example.com","id":"inq_victim","status":"qualified"}`))
	if w.Code != http.StatusOK {
		t.Fatalf("got %d: %s", w.Code, w.Body.String())
	}
	var resp map[string]any
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["id"] == "inq_victim" || resp["id"] == nil {
		t.Errorf("the id must be generated by the server: %v", resp["id"])
	}
	if _, echoed := resp["inquiry"]; echoed {
		t.Error("the stored inquiry should not be echoed back")
	}
}

func TestInquiryCORSAllowsOnlyTheMarketingSite(t *testing.T) {
	ok := postInquiry(t, "https://joltrinhq.com", []byte(`{"email":"a@example.com"}`))
	if got := ok.Header().Get("Access-Control-Allow-Origin"); got != "https://joltrinhq.com" {
		t.Errorf("the site origin should be allowed, got %q", got)
	}
	other := postInquiry(t, "https://evil.example", []byte(`{"email":"a@example.com"}`))
	if got := other.Header().Get("Access-Control-Allow-Origin"); got != "" {
		t.Errorf("another origin must not be allowed, got %q", got)
	}
	if other.Header().Get("Vary") == "" {
		t.Error("Vary: Origin is needed so caches do not mix the two answers")
	}
}

func TestClientIPKeyIgnoresWhatTheClientSendsInXForwardedFor(t *testing.T) {
	req := func(remote string, xff ...string) *http.Request {
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = remote
		for _, v := range xff {
			r.Header.Add("X-Forwarded-For", v)
		}
		return r
	}
	cases := []struct {
		name string
		r    *http.Request
		want string
	}{
		{"the ingress appends the real client last", req("10.0.0.1:5000", "6.6.6.6, 203.0.113.9"), "203.0.113.9"},
		{"a spoofed first entry changes nothing", req("10.0.0.1:5000", "1.1.1.1, 203.0.113.9"), "203.0.113.9"},
		{"a single entry", req("10.0.0.1:5000", "203.0.113.9"), "203.0.113.9"},
		{"spaces around entries", req("10.0.0.1:5000", "  6.6.6.6 ,   203.0.113.9  "), "203.0.113.9"},
		{"the last header line wins", req("10.0.0.1:5000", "6.6.6.6", "203.0.113.9"), "203.0.113.9"},
		{"no header uses the connection address without the port", req("198.51.100.4:61234"), "198.51.100.4"},
		{"a garbage last entry falls back to the connection", req("198.51.100.4:61234", "203.0.113.9, nonsense"), "198.51.100.4"},
		{"ipv6 is grouped by /64", req("[::1]:1", "2001:db8:1:2:aaaa:bbbb:cccc:dddd"), "2001:db8:1:2::/64"},
	}
	for _, c := range cases {
		if got := clientIPKey(c.r); got != c.want {
			t.Errorf("%s: got %q, want %q", c.name, got, c.want)
		}
	}
	if clientIPKey(req("10.0.0.1:1", "2001:db8:1:2:1::1")) != clientIPKey(req("10.0.0.1:2", "2001:db8:1:2:ffff::2")) {
		t.Error("two addresses in one /64 must share a bucket")
	}
	if clientIPKey(req("198.51.100.4:1")) != clientIPKey(req("198.51.100.4:2")) {
		t.Error("the same host on different ports must share a bucket")
	}
}

func TestRateLimitCannotBeEvadedByRotatingTheFirstForwardedAddress(t *testing.T) {
	send := func(fake string) int {
		r := httptest.NewRequest(http.MethodPost, "/api/billing/enterprise-contact", strings.NewReader(`{"email":"a@example.com"}`))
		r.Header.Set("X-Forwarded-For", fake+", 203.0.113.250")
		w := httptest.NewRecorder()
		handleSubmitEnterpriseInquiry(w, r)
		return w.Code
	}
	limited := false
	for i := 0; i < 200; i++ {
		if send(fmt.Sprintf("10.9.%d.%d", i/250, i%250)) == http.StatusTooManyRequests {
			limited = true
			break
		}
	}
	if !limited {
		t.Error("200 requests from one real client with a different spoofed first address each must hit the limit")
	}
}
