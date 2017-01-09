package core

import (
	"encoding/json"
	"sync"

	"golang.org/x/net/context"

	"chain/core/pb"
	"chain/core/signers"
	cjson "chain/encoding/json"
	"chain/net/http/reqid"
)

// This type enforces JSON field ordering in API output.
type assetResponse struct {
	ID              cjson.HexBytes  `json:"id"`
	Alias           string          `json:"alias"`
	IssuanceProgram cjson.HexBytes  `json:"issuance_program"`
	Keys            []*assetKey     `json:"keys"`
	Quorum          int32           `json:"quorum"`
	Definition      json.RawMessage `json:"definition"`
	Tags            json.RawMessage `json:"tags"`
	IsLocal         bool            `json:"is_local"`
}

type assetKey struct {
	RootXPub            cjson.HexBytes   `json:"root_xpub"`
	AssetPubkey         cjson.HexBytes   `json:"asset_pubkey"`
	AssetDerivationPath []cjson.HexBytes `json:"asset_derivation_path"`
}

func (h *Handler) CreateAssets(ctx context.Context, in *pb.CreateAssetsRequest) (*pb.CreateAssetsResponse, error) {
	responses := make([]*pb.CreateAssetsResponse_Response, len(in.Requests))
	var wg sync.WaitGroup
	wg.Add(len(responses))

	for i := range responses {
		go func(i int) {
			subctx := reqid.NewSubContext(ctx, reqid.New())
			defer wg.Done()
			defer batchRecover(func(err error) {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
			})

			var tags, def map[string]interface{}
			err := json.Unmarshal(in.Requests[i].Tags, &tags)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}
			err = json.Unmarshal(in.Requests[i].Definition, &def)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}

			xpubs, err := bytesToKeys(in.Requests[i].RootXpubs)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}

			asset, err := h.Assets.Define(
				subctx,
				xpubs,
				int(in.Requests[i].Quorum),
				def,
				in.Requests[i].Alias,
				tags,
				in.Requests[i].ClientToken,
			)
			if err != nil {
				detailedErr, _ := errInfo(err)
				responses[i] = &pb.CreateAssetsResponse_Response{
					Error: protobufErr(detailedErr),
				}
				return
			}
			var keys []*pb.Asset_Key
			for _, xpub := range asset.Signer.XPubs {
				path := signers.Path(asset.Signer, signers.AssetKeySpace)
				derived := xpub.Derive(path)
				keys = append(keys, &pb.Asset_Key{
					AssetPubkey:         derived[:],
					RootXpub:            xpub[:],
					AssetDerivationPath: path,
				})
			}
			responses[i] = &pb.CreateAssetsResponse_Response{
				Asset: &pb.Asset{
					Id:              asset.AssetID[:],
					IssuanceProgram: asset.IssuanceProgram,
					Keys:            keys,
					Quorum:          int32(asset.Signer.Quorum),
					Definition:      in.Requests[i].Definition,
					Tags:            in.Requests[i].Tags,
					IsLocal:         true,
				},
			}
		}(i)
	}

	wg.Wait()
	return &pb.CreateAssetsResponse{Responses: responses}, nil
}
