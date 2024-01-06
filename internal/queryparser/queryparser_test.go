package queryparser_test

import (
	"testing"

	"github.com/akrennmair/updog/internal/queryparser"
	proto "github.com/akrennmair/updog/proto/updog/v1"
	"github.com/stretchr/testify/require"
)

func TestValidQueryStrings(t *testing.T) {
	testData := []struct {
		QueryString   string
		ExpectedQuery *proto.Query
	}{
		{
			QueryString: `foo = "bar"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Eq{
						Eq: &proto.Query_Expression_Equal{
							Column: "foo",
							Value:  "bar",
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "foo""bar"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Eq{
						Eq: &proto.Query_Expression_Equal{
							Column: "foo",
							Value:  `foo"bar`,
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "bar" & bar = "baz"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_And_{
						And: &proto.Query_Expression_And{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "foo",
											Value:  "bar",
										},
									},
								},
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "bar",
											Value:  "baz",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "bar" & bar = "baz" & baz = "quux"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_And_{
						And: &proto.Query_Expression_And{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "foo",
											Value:  "bar",
										},
									},
								},
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "bar",
											Value:  "baz",
										},
									},
								},
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "baz",
											Value:  "quux",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "bar" & ( bar = "baz" | baz = "quux" )`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_And_{
						And: &proto.Query_Expression_And{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "foo",
											Value:  "bar",
										},
									},
								},
								{
									Value: &proto.Query_Expression_Or_{
										Or: &proto.Query_Expression_Or{
											Exprs: []*proto.Query_Expression{
												{
													Value: &proto.Query_Expression_Eq{
														Eq: &proto.Query_Expression_Equal{
															Column: "bar",
															Value:  "baz",
														},
													},
												},
												{
													Value: &proto.Query_Expression_Eq{
														Eq: &proto.Query_Expression_Equal{
															Column: "baz",
															Value:  "quux",
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "bar" | ( bar = "baz" & baz = "quux" )`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Or_{
						Or: &proto.Query_Expression_Or{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "foo",
											Value:  "bar",
										},
									},
								},
								{
									Value: &proto.Query_Expression_And_{
										And: &proto.Query_Expression_And{
											Exprs: []*proto.Query_Expression{
												{
													Value: &proto.Query_Expression_Eq{
														Eq: &proto.Query_Expression_Equal{
															Column: "bar",
															Value:  "baz",
														},
													},
												},
												{
													Value: &proto.Query_Expression_Eq{
														Eq: &proto.Query_Expression_Equal{
															Column: "baz",
															Value:  "quux",
														},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `^ foo = "bar"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Not_{
						Not: &proto.Query_Expression_Not{
							Expr: &proto.Query_Expression{
								Value: &proto.Query_Expression_Eq{
									Eq: &proto.Query_Expression_Equal{
										Column: "foo",
										Value:  "bar",
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `^ ( foo = "bar" & bar = "baz" )`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Not_{
						Not: &proto.Query_Expression_Not{
							Expr: &proto.Query_Expression{
								Value: &proto.Query_Expression_And_{
									And: &proto.Query_Expression_And{
										Exprs: []*proto.Query_Expression{
											{
												Value: &proto.Query_Expression_Eq{
													Eq: &proto.Query_Expression_Equal{
														Column: "foo",
														Value:  "bar",
													},
												},
											},
											{
												Value: &proto.Query_Expression_Eq{
													Eq: &proto.Query_Expression_Equal{
														Column: "bar",
														Value:  "baz",
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `^ foo = "bar" & bar = "baz"`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_And_{
						And: &proto.Query_Expression_And{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Not_{
										Not: &proto.Query_Expression_Not{
											Expr: &proto.Query_Expression{
												Value: &proto.Query_Expression_Eq{
													Eq: &proto.Query_Expression_Equal{
														Column: "foo",
														Value:  "bar",
													},
												},
											},
										},
									},
								},
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column: "bar",
											Value:  "baz",
										},
									},
								},
							},
						},
					},
				},
			},
		},
		{
			QueryString: `foo = "bar" ; bar, baz, quux`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Eq{
						Eq: &proto.Query_Expression_Equal{
							Column: "foo",
							Value:  "bar",
						},
					},
				},
				GroupBy: []string{"bar", "baz", "quux"},
			},
		},
		{
			QueryString: `foo = $1`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_Eq{
						Eq: &proto.Query_Expression_Equal{
							Column:      "foo",
							Placeholder: 1,
						},
					},
				},
			},
		},
		{
			QueryString: `foo = $1 & bar = $2`,
			ExpectedQuery: &proto.Query{
				Expr: &proto.Query_Expression{
					Value: &proto.Query_Expression_And_{
						And: &proto.Query_Expression_And{
							Exprs: []*proto.Query_Expression{
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column:      "foo",
											Placeholder: 1,
										},
									},
								},
								{
									Value: &proto.Query_Expression_Eq{
										Eq: &proto.Query_Expression_Equal{
											Column:      "bar",
											Placeholder: 2,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range testData {
		t.Run(tt.QueryString, func(t *testing.T) {
			q, err := queryparser.ParseQuery(tt.QueryString)
			require.NoError(t, err)
			require.NotNil(t, q)
			require.Equal(t, tt.ExpectedQuery, q)

			newQueryString := queryparser.QueryToString(q)
			require.Equal(t, tt.QueryString, newQueryString)
		})
	}
}

func TestInvalidQueryStrings(t *testing.T) {
	testData := []struct {
		Query string
	}{
		{"a = "},
		{`(a = "b"`},
		{`a ^ "b"`},
		{`a = "b" ; "c"`},
		{`a = "b" ; c, d, ^`},
		{`!`},
		{"a = $fart"},
		{"b = $0"},
	}

	for _, tt := range testData {
		t.Run(tt.Query, func(t *testing.T) {
			q, err := queryparser.ParseQuery(tt.Query)
			require.Error(t, err)
			require.Nil(t, q)
			t.Logf("query = %s", tt.Query)
			t.Logf("error = %v", err)
		})
	}
}
