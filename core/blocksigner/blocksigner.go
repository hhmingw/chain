package blocksigner

import (
	"bytes"
	"context"

	"chain/core/mockhsm"
	"chain/crypto/ed25519/chainkd"
	"chain/database/pg"
	"chain/errors"
	"chain/protocol"
	"chain/protocol/bc"
)

// ErrConsensusChange is returned from ValidateAndSignBlock
// when a new consensus program is detected.
var ErrConsensusChange = errors.New("consensus program has changed")

// Signer validates and signs blocks.
type Signer struct {
	XPub chainkd.XPub
	hsm  *mockhsm.HSM
	db   pg.DB
	c    *protocol.Chain
}

// New returns a new Signer that validates blocks with c and signs
// them with k.
func New(xpub chainkd.XPub, hsm *mockhsm.HSM, db pg.DB, c *protocol.Chain) *Signer {
	return &Signer{
		XPub: xpub,
		hsm:  hsm,
		db:   db,
		c:    c,
	}
}

// SignBlock computes the signature for the block using
// the private key in s.  It does not validate the block.
func (s *Signer) SignBlock(ctx context.Context, b *bc.Block) ([]byte, error) {
	hash := b.HashForSig()
	return s.hsm.SignWithChainKDKey(ctx, s.XPub, nil, hash[:]) // TODO(tessr): Should sign with Ed25519 key.
}

func (s *Signer) String() string {
	return "signer for key " + s.XPub.String()
}

// ValidateAndSignBlock validates the given block against the current blockchain
// and, if valid, computes and returns a signature for the block.  It
// is used as the httpjson handler for /rpc/signer/sign-block.
//
// This function fails if this node has ever signed a block at the
// same height as b.
func (s *Signer) ValidateAndSignBlock(ctx context.Context, b *bc.Block) ([]byte, error) {
	err := s.c.WaitForBlockSoon(ctx, b.Height-1)
	if err != nil {
		return nil, errors.Wrapf(err, "waiting for block at height %d", b.Height-1)
	}
	prev, err := s.c.GetBlock(ctx, b.Height-1)
	if err != nil {
		return nil, errors.Wrap(err, "getting block at height %d", b.Height-1)
	}
	// TODO: Add the ability to change the consensus program
	// by having a current consensus program, and a potential
	// next consensus program. Once the next consensus program
	// has been used, it will become the current consensus program
	// and the only signable consensus program until a new
	// next is set.
	if !bytes.Equal(b.ConsensusProgram, prev.ConsensusProgram) {
		return nil, errors.Wrap(ErrConsensusChange)
	}
	err = s.c.ValidateBlockForSig(ctx, b)
	if err != nil {
		return nil, errors.Wrap(err, "validating block for signature")
	}
	err = lockBlockHeight(ctx, s.db, b)
	if err != nil {
		return nil, errors.Wrap(err, "lock block height")
	}
	return s.SignBlock(ctx, b)
}

// lockBlockHeight records a signer's intention to sign a given block
// at a given height.  It's an error if a different block at the same
// height has previously been signed.
func lockBlockHeight(ctx context.Context, db pg.DB, b *bc.Block) error {
	const q = `
		INSERT INTO signed_blocks (block_height, block_hash)
		SELECT $1, $2
		    WHERE NOT EXISTS (SELECT 1 FROM signed_blocks
		                      WHERE block_height = $1 AND block_hash = $2)
	`
	_, err := db.Exec(ctx, q, b.Height, b.HashForSig())
	return err
}
